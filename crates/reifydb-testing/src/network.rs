// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	net::SocketAddr,
	time::{Duration, Instant},
};

pub fn busy_wait(f: impl Fn() -> Option<SocketAddr>) -> SocketAddr {
	let mut socket_addr: Option<SocketAddr>;
	let start = Instant::now();
	loop {
		socket_addr = f();
		if socket_addr.is_some() {
			break;
		}

		if start.elapsed() > Duration::from_millis(500) {
			panic!("failed to connect within 500ms")
		}
	}
	socket_addr.take().unwrap()
}
