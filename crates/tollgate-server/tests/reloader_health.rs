mod common;

use std::sync::Arc;
use tollgate_server::config::{SecurityLoader, SecurityReloader};
use tollgate_store::Clock;
use tracing_subscriber::layer::SubscriberExt;

struct PanicClock(Arc<tokio::sync::Notify>);
impl Clock for PanicClock {
    fn now(&self) -> jiff::Timestamp {
        self.0.notify_one();
        panic!("public reloader panic fixture")
    }
}

#[tokio::test(start_paused = true)]
async fn a_dead_security_reloader_reports_a_safe_error() {
    let capture = common::EventCapture::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("security.json");
    std::fs::write(&path, "{}").unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let reloader = SecurityReloader::spawn(
        SecurityLoader::new(path),
        common::security(),
        Arc::new(PanicClock(Arc::clone(&entered))),
    );
    tokio::time::timeout(std::time::Duration::from_secs(6), entered.notified())
        .await
        .unwrap();
    let events = capture.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].level, tracing::Level::ERROR);
    assert_eq!(events[0].fields["operation"], "security-reload");
    assert_eq!(events[0].fields["reason"], "unexpected-exit");
    assert!(!format!("{events:?}").contains("public reloader panic fixture"));
    drop(reloader);
}

#[tokio::test(start_paused = true)]
async fn dropping_an_unpolled_reloader_is_an_expected_stop() {
    let capture = common::EventCapture::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let clock = Arc::new(tollgate_store::SystemClock);
    let retained = Arc::downgrade(&clock);
    let reloader = SecurityReloader::spawn(
        SecurityLoader::new("unused-fixture-path"),
        common::security(),
        clock,
    );
    drop(reloader);
    tokio::task::yield_now().await;
    assert!(retained.upgrade().is_none());
    assert!(capture.events().is_empty());
}
