//! A TCP hop a scenario can break, so a network fault reaches real processes
//! without their cooperation.

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    Open,
    Stalled,
    Down,
}

/// Forwards connections to one upstream address until a scenario says
/// otherwise. Dropping it closes the listener.
pub struct Link {
    address: SocketAddr,
    state: watch::Sender<State>,
    accept: JoinHandle<()>,
}

impl Link {
    /// Listen on a loopback port the kernel chooses, forwarding to `upstream`.
    ///
    /// # Panics
    ///
    /// When no loopback port can be bound.
    pub async fn to(upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let address = listener.local_addr().expect("a bound address");
        let (state, watched) = watch::channel(State::Open);
        let accept = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                tokio::spawn(forward(client, upstream.clone(), watched.clone()));
            }
        });
        Link {
            address,
            state,
            accept,
        }
    }

    /// Where a process connects to use this link.
    #[must_use]
    pub fn address(&self) -> String {
        self.address.to_string()
    }

    /// A partition: connections stay open and open anew, but nothing crosses.
    pub fn stall(&self) {
        self.state.send_replace(State::Stalled);
    }

    /// An outage: connections are closed, and new ones are closed on arrival.
    pub fn cut(&self) {
        self.state.send_replace(State::Down);
    }

    /// Carry traffic again. Bytes held by a stall are delivered.
    pub fn heal(&self) {
        self.state.send_replace(State::Open);
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        self.accept.abort();
        // Ends the forwarding tasks as well.
        self.cut();
    }
}

/// Hold while the link is stalled; `false` once it is down or gone.
async fn passes(state: &mut watch::Receiver<State>) -> bool {
    state
        .wait_for(|state| *state != State::Stalled)
        .await
        .is_ok_and(|state| *state == State::Open)
}

async fn forward(client: TcpStream, upstream: String, mut state: watch::Receiver<State>) {
    if !passes(&mut state).await {
        return;
    }
    let Ok(server) = TcpStream::connect(upstream).await else {
        return;
    };
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    let mut down = state.clone();
    // Either direction ending, or the link going down, closes both.
    tokio::select! {
        () = carry(client_read, server_write, state.clone()) => {}
        () = carry(server_read, client_write, state) => {}
        _ = down.wait_for(|state| *state == State::Down) => {}
    }
}

async fn carry(
    mut from: impl AsyncReadExt + Unpin,
    mut to: impl AsyncWriteExt + Unpin,
    mut state: watch::Receiver<State>,
) {
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = match from.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        if !passes(&mut state).await {
            return;
        }
        if to.write_all(&buffer[..read]).await.is_err() {
            return;
        }
    }
}
