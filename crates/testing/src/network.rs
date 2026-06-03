// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{net::SocketAddr, time::Instant};

use reifydb_value::value::duration::Duration;

pub fn busy_wait(f: impl Fn() -> Option<SocketAddr>) -> SocketAddr {
	let mut socket_addr: Option<SocketAddr>;
	#[allow(clippy::disallowed_methods)]
	let start = Instant::now();
	loop {
		socket_addr = f();
		if socket_addr.is_some() {
			break;
		}

		if start.elapsed() > Duration::from_milliseconds(500).unwrap().to_std() {
			panic!("failed to connect within 500ms")
		}
	}
	socket_addr.take().unwrap()
}
