//! Blocking HTTP clients and their owned warmup/measurement rendezvous.
//! Operational failures are values, including when compiled with panic=abort.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

pub(super) struct ClientWork {
    pub request: String,
    pub warmup: usize,
    pub measured: usize,
}

#[derive(Debug, Default)]
pub(super) struct ClientOutcome {
    pub samples: Vec<u64>,
    pub shed: usize,
}

#[derive(Debug)]
pub(super) struct MeasuredSamples {
    pub outcomes: Vec<ClientOutcome>,
    pub throughput_requests_per_second: f64,
}

#[derive(Clone)]
struct MeasurementGate(Arc<(Mutex<Option<bool>>, Condvar)>);

impl MeasurementGate {
    fn wait(&self) -> Result<bool, String> {
        let (lock, wake) = &*self.0;
        let mut state = lock.lock().map_err(|_| "measurement gate poisoned")?;
        loop {
            if let Some(run) = *state {
                return Ok(run);
            }
            state = wake.wait(state).map_err(|_| "measurement gate poisoned")?;
        }
    }

    fn release(&self, run: bool) -> Result<(), String> {
        let (lock, wake) = &*self.0;
        let result = match lock.lock() {
            Ok(mut state) => {
                *state = Some(run);
                Ok(())
            }
            Err(poisoned) => {
                // Refuse the measurement and still wake every waiter. Clearing
                // poison would silently turn uncertain state into authority.
                *poisoned.into_inner() = Some(false);
                Err("measurement gate poisoned".to_owned())
            }
        };
        wake.notify_all();
        result
    }
}

/// The sole coordinator owns cancellation even if its async future is dropped.
struct Coordinator(MeasurementGate);

impl Coordinator {
    fn new() -> Self {
        Self(MeasurementGate(Arc::new((
            Mutex::new(None),
            Condvar::new(),
        ))))
    }
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        let _ = self.0.release(false);
    }
}

pub(super) async fn drive_clients(
    address: SocketAddr,
    work: Vec<ClientWork>,
) -> Result<MeasuredSamples, String> {
    let connections = work.len();
    if connections == 0 {
        return Err("load-gate requires at least one client".to_owned());
    }
    let coordinator = Coordinator::new();
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut clients = Vec::with_capacity(connections);
    for work in work {
        let gate = coordinator.0.clone();
        let ready_tx = ready_tx.clone();
        clients.push(tokio::task::spawn_blocking(move || {
            run_client(address, work, ready_tx, gate)
        }));
    }
    drop(ready_tx);

    // Bounded by the client count rather than by a comparison, so the release
    // below cannot wait on a channel close that a stuck client never performs.
    let mut ready = 0;
    for _ in 0..connections {
        if ready_rx.recv().await.is_none() {
            break;
        }
        ready += 1;
    }
    let all_ready = ready == connections;
    let measured_started = Instant::now();
    let released = coordinator.0.release(all_ready);
    let mut outcomes = Vec::with_capacity(connections);
    let mut errors = Vec::new();
    for (index, client) in clients.into_iter().enumerate() {
        match client.await {
            Ok(Ok(outcome)) => outcomes.push(outcome),
            Ok(Err(error)) => errors.push(format!("client {index}: {error}")),
            // This arm handles task cancellation/unwinding only. I/O and
            // protocol errors arrive through Ok(Err), in every panic profile.
            Err(_) => errors.push(format!("client {index}: task did not complete")),
        }
    }
    let measured_elapsed = measured_started.elapsed();
    if let Err(error) = released {
        errors.push(error);
    }
    if !all_ready {
        errors.push(format!(
            "only {ready} of {connections} clients completed warmup"
        ));
    }
    if !errors.is_empty() {
        return Err(errors.join("; "));
    }
    // Only served requests count; a refusal cannot improve throughput.
    let served = outcomes.iter().map(|o| o.samples.len()).sum();
    Ok(MeasuredSamples {
        outcomes,
        throughput_requests_per_second: throughput_for_window(
            served,
            measured_elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
        ),
    })
}

pub(super) fn throughput_for_window(requests: usize, elapsed_seconds: f64) -> f64 {
    (requests as f64) / elapsed_seconds
}

const IO_TIMEOUT: Duration = Duration::from_secs(10);
// The example admits at most 1,024 contracts. At 24 bytes per JSON f64,
// plus metadata and bounded headers, 64 KiB admits its largest price reply.
// Allocate once per connection, outside the measured window.
const RESPONSE_CAPACITY: usize = 64 * 1024;

pub(super) async fn wait_ready(address: SocketAddr, timeout: Duration) -> Result<(), String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tokio::time::timeout(timeout, async {
        let mut buf = vec![0; RESPONSE_CAPACITY];
        loop {
            if let Ok(mut stream) = tokio::net::TcpStream::connect(address).await {
                let request =
                    format!("GET /readyz HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
                if stream.write_all(request.as_bytes()).await.is_ok() {
                    let mut response = Vec::new();
                    // Retry startup refusals, but never absorb partial I/O,
                    // an oversized reply or a peer that never closes. The
                    // whole readiness phase has one deadline, including I/O.
                    if stream
                        .take(RESPONSE_CAPACITY as u64 + 1)
                        .read_to_end(&mut response)
                        .await
                        .is_ok()
                        && response.len() <= RESPONSE_CAPACITY
                        && read_response(&mut response.as_slice(), &mut buf) == Ok(Outcome::Served)
                    {
                        return;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| "service did not become ready within its deadline".to_owned())
}

fn run_client(
    address: SocketAddr,
    work: ClientWork,
    ready_tx: tokio::sync::mpsc::UnboundedSender<()>,
    gate: MeasurementGate,
) -> Result<ClientOutcome, String> {
    let mut stream = TcpStream::connect_timeout(&address, IO_TIMEOUT)
        .map_err(|error| format!("connect: {error}"))?;
    stream
        .set_nodelay(true)
        .map_err(|error| format!("set TCP_NODELAY: {error}"))?;
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .map_err(|error| format!("set read timeout: {error}"))?;
    stream
        .set_write_timeout(Some(IO_TIMEOUT))
        .map_err(|error| format!("set write timeout: {error}"))?;
    let mut buf = vec![0u8; RESPONSE_CAPACITY];
    for _ in 0..work.warmup {
        exchange(&mut stream, &work.request, &mut buf)
            .map_err(|error| format!("warmup: {error}"))?;
    }
    ready_tx
        .send(())
        .map_err(|_| "load-gate coordinator stopped")?;
    drop(ready_tx);
    if !gate.wait()? {
        return Ok(ClientOutcome::default());
    }
    let mut outcome = ClientOutcome {
        samples: Vec::with_capacity(work.measured),
        shed: 0,
    };
    for _ in 0..work.measured {
        let start = Instant::now();
        match exchange(&mut stream, &work.request, &mut buf)
            .map_err(|error| format!("measured: {error}"))?
        {
            Outcome::Served => outcome
                .samples
                .push(u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)),
            Outcome::Shed => outcome.shed += 1,
        }
    }
    Ok(outcome)
}

fn exchange(
    stream: &mut (impl Read + Write),
    request: &str,
    buf: &mut [u8],
) -> Result<Outcome, String> {
    stream
        .write_all(request.as_bytes())
        .map_err(|error| format!("write request: {error}"))?;
    read_response(stream, buf)
}

#[derive(Debug, PartialEq)]
enum Outcome {
    Served,
    Shed,
}

fn read_more(stream: &mut impl Read, buf: &mut [u8]) -> Result<usize, String> {
    loop {
        match stream.read(buf) {
            Ok(0) => return Err("server closed an incomplete response".to_owned()),
            Ok(n) => return Ok(n),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(format!("read response: {error}")),
        }
    }
}

/// The local service sends HTTP/1.1 with Content-Length, never chunked. Reject
/// unsupported framing explicitly and never copy response text into diagnostics.
fn read_response(stream: &mut impl Read, buf: &mut [u8]) -> Result<Outcome, String> {
    let mut filled = 0;
    let mut search_from = 0;
    let header_end = loop {
        if filled == buf.len() || filled >= 16 * 1024 {
            return Err("response headers exceed the buffer limit".to_owned());
        }
        filled += read_more(stream, &mut buf[filled..])?;
        if let Some(offset) = buf[search_from..filled]
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
        {
            let end = search_from + offset;
            if end + 4 > 16 * 1024 {
                return Err("response headers exceed the buffer limit".to_owned());
            }
            break end;
        }
        search_from = filled.saturating_sub(3);
    };
    let headers =
        std::str::from_utf8(&buf[..header_end]).map_err(|_| "invalid response headers")?;
    let mut lines = headers.split("\r\n");
    let mut status_line = lines.next().unwrap_or_default().splitn(3, ' ');
    if status_line.next() != Some("HTTP/1.1") {
        return Err("unsupported HTTP response version".to_owned());
    }
    let status = status_line
        .next()
        .filter(|s| s.len() == 3 && s.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or("invalid HTTP response status")?;
    if status != 200 && status != 503 {
        return Err(format!("unexpected HTTP status {status}"));
    }
    let mut content_length = None;
    for line in lines {
        let (name, value) = line.split_once(':').ok_or("malformed response header")?;
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err("unsupported transfer-encoding".to_owned());
        }
        if name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some() {
                return Err("duplicate content-length".to_owned());
            }
            let value = value.trim();
            if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                return Err("invalid content-length".to_owned());
            }
            content_length = Some(
                value
                    .parse::<usize>()
                    .map_err(|_| "invalid content-length")?,
            );
        }
    }
    let body_start = header_end + 4;
    let total = body_start
        .checked_add(content_length.ok_or("missing content-length")?)
        .ok_or("response length overflow")?;
    if total > buf.len() {
        return Err("response exceeds the buffer limit".to_owned());
    }
    if filled > total {
        return Err("unexpected bytes after the response".to_owned());
    }
    while filled < total {
        filled += read_more(stream, &mut buf[filled..total])?;
    }
    if status == 200 {
        return Ok(Outcome::Served);
    }
    #[derive(serde::Deserialize)]
    struct Refusal<'a> {
        code: &'a str,
    }
    let refusal: Refusal<'_> = serde_json::from_slice(&buf[body_start..total])
        .map_err(|_| "invalid HTTP 503 problem body")?;
    if refusal.code == "capacity-unavailable" {
        Ok(Outcome::Shed)
    } else {
        Err("HTTP 503 was not a capacity refusal".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn readiness_io_cannot_outlive_the_startup_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
            drop(stream);
        });
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            wait_ready(address, Duration::from_millis(30)),
        )
        .await
        .unwrap();
        assert!(result.unwrap_err().contains("deadline"));
        peer.abort();
        assert!(peer.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn readiness_retries_incomplete_responses_until_a_complete_success() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            for response in [
                "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nx",
                "HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n",
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}",
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(stream.read_u8().await.unwrap());
                }
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        wait_ready(address, Duration::from_secs(1)).await.unwrap();
        peer.await.unwrap();
    }

    /// A valid readiness reply of exactly `RESPONSE_CAPACITY` bytes.
    fn largest_ready_response() -> Vec<u8> {
        let head = b"HTTP/1.1 200 OK\r\nContent-Length: 65494\r\n\r\n";
        let mut response = head.to_vec();
        response.resize(RESPONSE_CAPACITY, b'x');
        assert_eq!(response.len(), RESPONSE_CAPACITY);
        response
    }

    async fn ready_against(reply: Vec<u8>) -> Result<(), String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(stream.read_u8().await.unwrap());
            }
            let _ = stream.write_all(&reply).await;
            drop(stream);
            // Later attempts are accepted and never answered, so a rejected
            // first reply ends at the readiness deadline rather than by luck.
            std::future::pending::<()>().await;
        });
        let result = wait_ready(address, Duration::from_millis(250)).await;
        peer.abort();
        result
    }

    #[tokio::test]
    async fn readiness_admits_the_largest_reply_and_refuses_one_byte_more() {
        ready_against(largest_ready_response()).await.unwrap();

        let mut oversized = largest_ready_response();
        oversized.push(b'x');
        assert!(
            ready_against(oversized)
                .await
                .unwrap_err()
                .contains("deadline"),
            "a reply past the buffer must never be read as readiness"
        );
    }

    #[tokio::test]
    async fn cancelling_the_driver_releases_ready_clients_and_refuses_late_readiness() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let driver = tokio::spawn(drive_clients(
            address,
            vec![
                ClientWork {
                    request: "GET /warmup HTTP/1.1\r\n\r\n".to_owned(),
                    warmup: 1,
                    measured: 1,
                },
                ClientWork {
                    request: "GET /measured HTTP/1.1\r\n\r\n".to_owned(),
                    warmup: 0,
                    measured: 1,
                },
            ],
        ));
        let (mut first, _) = listener.accept().await.unwrap();
        let (mut second, _) = listener.accept().await.unwrap();
        let mut first_byte = [0];
        let mut second_byte = [0];
        // Only the warming client can send before release; select identifies
        // it without relying on TCP accept order or a sleep.
        let (mut warming, mut ready, byte) = tokio::select! {
            result = first.read_exact(&mut first_byte) => { result.unwrap(); (first, second, first_byte[0]) }
            result = second.read_exact(&mut second_byte) => { result.unwrap(); (second, first, second_byte[0]) }
        };
        let mut request = vec![byte];
        while !request.ends_with(b"\r\n\r\n") {
            request.push(warming.read_u8().await.unwrap());
        }
        driver.abort();
        assert!(driver.await.unwrap_err().is_cancelled());
        warming
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            assert_eq!(ready.read(&mut [0; 1]).await.unwrap(), 0);
            assert_eq!(warming.read(&mut [0; 1]).await.unwrap(), 0);
        })
        .await
        .expect("abandoned clients must close without sending measured work");
    }

    struct Fragmented<'a> {
        bytes: &'a [u8],
        width: usize,
    }
    impl Read for Fragmented<'_> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.bytes.len().min(buf.len()).min(self.width);
            buf[..n].copy_from_slice(&self.bytes[..n]);
            self.bytes = &self.bytes[n..];
            Ok(n)
        }
    }

    fn parse(bytes: &[u8], width: usize) -> Result<Outcome, String> {
        read_response(
            &mut Fragmented { bytes, width },
            &mut vec![0; RESPONSE_CAPACITY],
        )
    }

    /// A valid, empty-bodied response whose header block ends exactly at
    /// `header_end`, so the header budget can be probed at its boundary.
    fn padded_headers(header_end: usize) -> Vec<u8> {
        let prefix = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-Pad: ";
        let mut response = prefix.to_vec();
        response.resize(header_end, b'x');
        response.extend_from_slice(b"\r\n\r\n");
        response
    }

    #[test]
    fn the_header_budget_is_measured_at_its_declared_boundary() {
        // Headers well inside the budget are served however the reads split.
        assert_eq!(parse(&padded_headers(5_000), 512), Ok(Outcome::Served));
        // The last accepted block ends exactly on the budget.
        assert_eq!(
            parse(&padded_headers(16 * 1024 - 4), usize::MAX),
            Ok(Outcome::Served)
        );
        // One byte past it is refused, even though the response is otherwise
        // valid and fits the response buffer.
        assert!(
            parse(&padded_headers(16 * 1024 + 1), usize::MAX)
                .unwrap_err()
                .contains("headers exceed")
        );
    }

    #[test]
    fn response_framing_accepts_fragmentation_and_only_capacity_shedding() {
        let success = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
        let capacity = b"HTTP/1.1 503 Unavailable\r\ncontent-LENGTH: 31\r\n\r\n{\"code\":\"capacity-unavailable\"}";
        for width in 1..=success.len() {
            assert_eq!(parse(success, width), Ok(Outcome::Served));
            assert_eq!(parse(capacity, width), Ok(Outcome::Shed));
        }
        let body = "0".repeat(32 * 1024);
        let large = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        assert_eq!(parse(large.as_bytes(), 1024), Ok(Outcome::Served));
        let full_body = "x".repeat(100);
        let full = format!("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{full_body}");
        assert_eq!(
            read_response(&mut full.as_bytes(), &mut vec![0; full.len()]),
            Ok(Outcome::Served)
        );
    }

    #[test]
    fn invalid_responses_are_errors_without_response_payloads() {
        for (bytes, expected) in [
            (&b"HTTP/1.1 200 OK\r\n"[..], "incomplete"),
            (&b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n{}"[..], "incomplete"),
            (&b"HTTP/1.1 2000 payload-secret\r\nContent-Length: 0\r\n\r\n"[..], "status"),
            // Three characters and three digits are both required: a padded
            // success code is not a status this client may act on.
            (
                &b"HTTP/1.1 0200 payload-secret\r\nContent-Length: 0\r\n\r\n"[..],
                "invalid HTTP response status",
            ),
            (&b"HTTP/1.0 200 payload-secret\r\nContent-Length: 0\r\n\r\n"[..], "version"),
            (&b"HTTP/1.1 500 payload-secret\r\nContent-Length: 0\r\n\r\n"[..], "500"),
            (&b"HTTP/1.1 200 OK\r\nMissing-colon\r\n\r\n"[..], "malformed"),
            (&b"HTTP/1.1 200 OK\r\n\r\n"[..], "missing content-length"),
            (&b"HTTP/1.1 200 OK\r\nContent-Length: +1\r\n\r\nx"[..], "invalid content-length"),
            (&b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n"[..], "duplicate"),
            (&b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n"[..], "transfer-encoding"),
            (&b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\nx"[..], "unexpected bytes"),
            (&b"HTTP/1.1 200 OK\r\nX: \xff\r\n\r\n"[..], "invalid response headers"),
            (&b"HTTP/1.1 503 Unavailable\r\nContent-Length: 2\r\n\r\n{}"[..], "problem body"),
            (&b"HTTP/1.1 503 Unavailable\r\nContent-Length: 28\r\n\r\n{\"code\":\"quota-unavailable\"}"[..], "not a capacity refusal"),
        ] {
            let error = parse(bytes, usize::MAX).unwrap_err();
            assert!(error.contains(expected), "expected {expected}: {error}");
            assert!(!error.contains("payload-secret"));
        }
        for length in [
            usize::MAX.to_string(),
            "99999999999999999999999999999".to_owned(),
            RESPONSE_CAPACITY.to_string(),
        ] {
            let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {length}\r\n\r\n");
            assert!(parse(response.as_bytes(), 10).is_err());
        }
        assert!(
            parse(&vec![b'x'; RESPONSE_CAPACITY], 1024)
                .unwrap_err()
                .contains("headers exceed")
        );
        assert!(
            read_response(&mut &b"anything"[..], &mut [])
                .unwrap_err()
                .contains("headers exceed")
        );
    }

    struct FailedIo;
    impl Read for FailedIo {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::TimedOut.into())
        }
    }
    impl Write for FailedIo {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn socket_failures_are_returned_at_the_io_boundary() {
        assert!(
            read_response(&mut FailedIo, &mut [0; 100])
                .unwrap_err()
                .contains("read response")
        );
        assert!(
            exchange(&mut FailedIo, "request", &mut [0; 100])
                .unwrap_err()
                .contains("write request")
        );
        struct Interrupted(bool);
        impl Read for Interrupted {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if std::mem::replace(&mut self.0, false) {
                    Err(std::io::ErrorKind::Interrupted.into())
                } else {
                    buf[0] = b'x';
                    Ok(1)
                }
            }
        }
        let mut byte = [0];
        assert_eq!(read_more(&mut Interrupted(true), &mut byte).unwrap(), 1);
        assert_eq!(byte, [b'x']);
    }

    #[test]
    fn measurement_gate_publishes_run_and_abort_decisions() {
        for decision in [true, false] {
            let coordinator = Coordinator::new();
            let waiter = coordinator.0.clone();
            let thread = std::thread::spawn(move || waiter.wait());
            coordinator.0.release(decision).unwrap();
            assert_eq!(thread.join().unwrap(), Ok(decision));
        }
    }

    #[test]
    fn coordinator_drop_releases_waiters_and_poison_is_an_error() {
        let coordinator = Coordinator::new();
        let waiter = coordinator.0.clone();
        let thread = std::thread::spawn(move || waiter.wait());
        drop(coordinator);
        assert_eq!(thread.join().unwrap(), Ok(false));

        let coordinator = Coordinator::new();
        let poisoned = coordinator.0.clone();
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned.0.0.lock().unwrap();
                panic!("fixture poisons coordination state");
            })
            .join()
            .is_err()
        );
        assert_eq!(
            coordinator.0.release(true),
            Err("measurement gate poisoned".to_owned())
        );
        assert_eq!(
            coordinator.0.wait(),
            Err("measurement gate poisoned".to_owned())
        );
        drop(coordinator);
    }
}
