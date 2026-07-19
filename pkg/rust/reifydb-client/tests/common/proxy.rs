// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A raw-TCP relay used to inject connection failures between a client and a live server.
//!
//! The server binds an ephemeral port, so restarting it to force a disconnect would change
//! the port and defeat reconnection (which dials the same URL). Instead the client connects
//! to this proxy, which forwards to the real server and can:
//! - `kill()` - drop every live connection, so the client observes a close and reconnects;
//! - `pause()` - refuse new connections, to exercise backoff and attempt-exhaustion.
//!
//! Raw TCP means one helper serves both the WebSocket and gRPC transports.

use std::sync::{
	Arc,
	atomic::{AtomicBool, Ordering},
};

use tokio::{
	net::{TcpListener, TcpStream},
	select, spawn,
	sync::broadcast,
};

pub struct TcpProxy {
	addr: String,
	paused: Arc<AtomicBool>,
	kill_tx: broadcast::Sender<()>,
}

impl TcpProxy {
	/// Start a relay on a fresh ephemeral `[::1]` port forwarding to `[::1]:upstream_port`.
	pub async fn start(upstream_port: u16) -> Self {
		let listener = TcpListener::bind("[::1]:0").await.unwrap();
		let port = listener.local_addr().unwrap().port();
		let paused = Arc::new(AtomicBool::new(false));
		let (kill_tx, _) = broadcast::channel(16);

		let accept_paused = paused.clone();
		let accept_kill = kill_tx.clone();
		spawn(async move {
			loop {
				let inbound = match listener.accept().await {
					Ok((stream, _)) => stream,
					Err(_) => continue,
				};
				if accept_paused.load(Ordering::SeqCst) {
					drop(inbound);
					continue;
				}
				spawn(relay(inbound, upstream_port, accept_kill.subscribe()));
			}
		});

		Self {
			addr: format!("[::1]:{}", port),
			paused,
			kill_tx,
		}
	}

	/// `host:port` the client should connect to (the proxy, not the server).
	pub fn addr(&self) -> &str {
		&self.addr
	}

	/// WebSocket URL pointing at the proxy.
	pub fn ws_url(&self) -> String {
		format!("ws://{}", self.addr)
	}

	/// Drop every live connection; connected clients see a close and reconnect.
	pub fn kill(&self) {
		let _ = self.kill_tx.send(());
	}

	/// Refuse new connections; existing connections are unaffected. Used to force reconnection
	/// attempts to fail so a client exhausts its retries.
	pub fn pause(&self) {
		self.paused.store(true, Ordering::SeqCst);
	}
}

async fn relay(inbound: TcpStream, upstream_port: u16, mut kill_rx: broadcast::Receiver<()>) {
	let upstream = match TcpStream::connect(format!("[::1]:{}", upstream_port)).await {
		Ok(stream) => stream,
		Err(_) => return,
	};

	let (mut client_read, mut client_write) = inbound.into_split();
	let (mut server_read, mut server_write) = upstream.into_split();

	select! {
		_ = tokio::io::copy(&mut client_read, &mut server_write) => {}
		_ = tokio::io::copy(&mut server_read, &mut client_write) => {}
		_ = kill_rx.recv() => {}
	}
}
