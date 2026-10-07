//! The process stops promptly on SIGTERM, even with a request still in flight.
//!
//! Kubernetes sends SIGTERM and kills the pod 30 s later; a writer that has not
//! exited by then loses its session mid-batch anyway. The binary runs against a
//! node that accepts connections and never answers, so a page view and the
//! indexer are both stuck in an RPC call when the signal arrives.
#![cfg(unix)]

use std::collections::BTreeSet;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A TCP server that accepts and then says nothing, for as long as the test runs.
fn silent_node() -> u16 {
    let listener = unclaimed_listener();
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
    unclaimed_listener().local_addr().expect("addr").port()
}

/// A listener on a port no other test here has had. The kernel can hand a
/// released port straight out again, and two tests on one port each reach
/// the other's explorer, which then signal too early.
fn unclaimed_listener() -> TcpListener {
    static CLAIMED: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
    loop {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        if CLAIMED.lock().expect("claimed ports").insert(port) {
            return listener;
        }
    }
}

/// The explorer under test, killed and reaped however the test ends, so a
/// failing test never leaves it running.
struct Explorer(Child);

impl std::ops::Deref for Explorer {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for Explorer {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for Explorer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// SIGTERM on cue, from a shell already up and waiting with its builtin
/// `kill`: a process started from a test can take a few hundred milliseconds
/// to run, most of the margin a test allows the explorer.
struct Sigterm(Child);

impl Sigterm {
    /// Returns once the shell is up.
    fn ready() -> Self {
        let mut shell = Command::new("sh")
            .args(["-c", "echo && read pid && kill -TERM \"$pid\""])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn the trigger");
        let mut up = [0];
        shell
            .stdout
            .take()
            .expect("stdout")
            .read_exact(&mut up)
            .expect("the trigger never started");
        Sigterm(shell)
    }

    /// Whether the signal went out.
    fn send(mut self, pid: u32) -> bool {
        let mut cue = self.0.stdin.take().expect("stdin");
        let cued = std::io::Write::write_all(&mut cue, format!("{pid}\n").as_bytes());
        drop(cue);
        cued.is_ok() && self.0.wait().is_ok_and(|status| status.success())
    }
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
    let mut child = Explorer(
        Command::new(env!("CARGO_BIN_EXE_nvnmchain-explorer"))
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
            .expect("spawn the explorer"),
    );
    wait_until_listening(port);
    let sigterm = Sigterm::ready();

    // A token page for an unknown address asks the node, which never answers.
    let mut in_flight = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    std::io::Write::write_all(
        &mut in_flight,
        b"GET /token/0x1111111111111111111111111111111111111111 HTTP/1.1\r\nHost: x\r\n\r\n",
    )
    .expect("send request");
    std::thread::sleep(Duration::from_millis(300));

    let started = Instant::now();
    assert!(sigterm.send(child.id()), "kill failed");
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

/// The 3 s budget holds even when the runtime cannot run: with one worker, and
/// the database write-locked by the test, the stats task's inline SQLite write
/// waits out the 5 s busy timeout on that worker, so nothing on the runtime
/// can see the signal or fire a timer until it returns.
#[test]
fn sigterm_exits_within_three_seconds_with_the_runtime_stuck_in_sqlite() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("stuck.db");
    let node = silent_node();
    let port = free_port();
    let mut child = Explorer(
        Command::new(env!("CARGO_BIN_EXE_nvnmchain-explorer"))
            .env("DB_PATH", &path)
            .env("TOKIO_WORKER_THREADS", "1")
            .env("STATS_INTERVAL_SECONDS", "1")
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
            .expect("spawn the explorer"),
    );
    wait_until_listening(port);
    wait_for_stats(&path);
    let sigterm = Sigterm::ready();

    // Within a second the next stats write starts, and waits on this lock.
    let lock = rusqlite::Connection::open(&path).expect("open");
    lock.execute_batch("BEGIN IMMEDIATE").expect("write lock");
    std::thread::sleep(Duration::from_millis(1_500));

    let started = Instant::now();
    let sent = sigterm.send(child.id());
    let deadline = started + Duration::from_secs(15);
    let status = loop {
        if let Some(status) = child.try_wait().expect("wait") {
            break Some(status);
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let took = started.elapsed();
    drop(lock);
    assert!(sent, "kill failed");
    let status = status.expect("still running 15 s after SIGTERM");
    assert!(status.success(), "a graceful stop, not a crash: {status}");
    assert!(
        took < Duration::from_millis(3_500),
        "exited {took:?} after SIGTERM"
    );
}

/// Wait until the stats task has written once: the database is open, in WAL
/// mode and with its schema, so a lock taken now holds up the next stats
/// write. Until the WAL file exists the test keeps off the database, since a
/// reader then could fail the explorer's switch to WAL.
fn wait_for_stats(path: &Path) {
    let wal = format!("{}-wal", path.display());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let written = Path::new(&wal).exists()
            && rusqlite::Connection::open(path)
                .and_then(|db| {
                    db.query_row("SELECT count(*) FROM kv WHERE key = 'stats'", [], |row| {
                        row.get::<_, i64>(0)
                    })
                })
                .is_ok_and(|n| n > 0);
        if written {
            return;
        }
        assert!(Instant::now() < deadline, "the stats task never wrote");
        std::thread::sleep(Duration::from_millis(20));
    }
}
