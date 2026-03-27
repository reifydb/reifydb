// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, ops::Range, time::Duration};

use crate::node::{NodeId, Ticks};

pub struct ClusterConfig {
	pub node_id: NodeId,
	pub bind_addr: SocketAddr,
	pub peers: Vec<PeerConfig>,
	pub tick_interval: Duration,
	pub election_timeout_range: Range<Ticks>,
	pub heartbeat_interval: Ticks,
}

pub struct PeerConfig {
	pub node_id: NodeId,
	pub addr: SocketAddr,
}

impl Default for ClusterConfig {
	fn default() -> Self {
		Self {
			node_id: 1,
			bind_addr: "127.0.0.1:9100".parse().unwrap(),
			peers: Vec::new(),
			tick_interval: Duration::from_millis(100),
			election_timeout_range: 10..20,
			heartbeat_interval: 4,
		}
	}
}
