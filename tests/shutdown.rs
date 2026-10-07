//! The process stops promptly on SIGTERM, even with a request still in flight.
//!
//! Kubernetes sends SIGTERM and kills the pod 30 s later; a writer that has not
//! exited by then loses its session mid-batch anyway. The binary runs against a
//! node that accepts connections and never answers, so a page view and the
//! indexer are both stuck in an RPC call when the signal arrives.
#![cfg(unix)]

use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// A TCP server that accepts and then says nothing, for as long as the test runs.
fn silent_node() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming().flatten() {
            held.push(stream);
        }
    });
    port
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn wait_until_listening(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "the explorer never listened");
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn sigterm_exits_within_three_seconds_with_a_request_in_flight() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = silent_node();
    let port = free_port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_nvnmchain-explorer"))
        .env("DB_PATH", dir.path().join("shutdown.db"))
        .env("HOST", "127.0.0.1")
        .env("PORT", port.to_string())
        .env("NVNM_RPC", format!("http://127.0.0.1:{node}"))
        .env("WS_URL", format!("ws://127.0.0.1:{node}"))
        .env("SIGNATURE_LOOKUP_URL", "")
        .env_remove("DATABASE_URL")
        .env_remove("ROLE")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the explorer");
    wait_until_listening(port);

    // A token page for an unknown address asks the node, which never answers.
    let mut in_flight = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    std::io::Write::write_all(
        &mut in_flight,
        b"GET /token/0x1111111111111111111111111111111111111111 HTTP/1.1\r\nHost: x\r\n\r\n",
    )
    .expect("send request");
    std::thread::sleep(Duration::from_millis(300));

    let started = Instant::now();
    let killed = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .expect("kill");
    assert!(killed.success());
    let deadline = started + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().expect("wait") {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("still running 10 s after SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let took = started.elapsed();
    assert!(status.success(), "a graceful stop, not a crash: {status}");
    assert!(
        took < Duration::from_millis(3_500),
        "exited {took:?} after SIGTERM ({status})"
    );
    // The request was cut off rather than answered.
    let mut rest = Vec::new();
    let _ = in_flight.read_to_end(&mut rest);
}
