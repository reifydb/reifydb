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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct RaftConfig {
	pub node_id: NodeId,
	pub bind_addr: SocketAddr,
	pub peers: Vec<PeerConfig>,
	pub heartbeat_interval: Ticks,
	pub election_timeout_range: Range<Ticks>,
	pub max_append_entries: usize,
	pub tick_interval: Duration,
	pub recv_interval: Duration,
	pub proposal_channel_capacity: usize,
}

impl Default for RaftConfig {
	fn default() -> Self {
		Self {
			node_id: 1,
			bind_addr: "127.0.0.1:9100".parse().unwrap(),
			peers: Vec::new(),
			heartbeat_interval: 3,
			election_timeout_range: 8..15,
			max_append_entries: 100,
			tick_interval: Duration::from_millis(50),
			recv_interval: Duration::from_millis(5),
			proposal_channel_capacity: 64,
		}
	}
}

impl RaftConfig {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn node_id(mut self, id: NodeId) -> Self {
		self.node_id = id;
		self
	}

	pub fn bind_addr(mut self, addr: impl Into<SocketAddr>) -> Self {
		self.bind_addr = addr.into();
		self
	}

	pub fn peer(mut self, id: NodeId, addr: impl Into<SocketAddr>) -> Self {
		self.peers.push(PeerConfig {
			node_id: id,
			addr: addr.into(),
		});
		self
	}

	pub fn heartbeat_interval(mut self, ticks: Ticks) -> Self {
		self.heartbeat_interval = ticks;
		self
	}

	pub fn election_timeout_range(mut self, range: Range<Ticks>) -> Self {
		self.election_timeout_range = range;
		self
	}

	pub fn max_append_entries(mut self, n: usize) -> Self {
		self.max_append_entries = n;
		self
	}

	pub fn tick_interval(mut self, interval: Duration) -> Self {
		self.tick_interval = interval;
		self
	}

	pub fn recv_interval(mut self, interval: Duration) -> Self {
		self.recv_interval = interval;
		self
	}

	pub fn proposal_channel_capacity(mut self, capacity: usize) -> Self {
		self.proposal_channel_capacity = capacity;
		self
	}
}
