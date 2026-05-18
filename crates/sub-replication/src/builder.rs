// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

pub struct PrimaryConfigurator {
	bind_addr: Option<String>,
	poll_interval: Duration,
	batch_size: u64,
}

impl Default for PrimaryConfigurator {
	fn default() -> Self {
		Self::new()
	}
}

impl PrimaryConfigurator {
	pub fn new() -> Self {
		Self {
			bind_addr: None,
			poll_interval: Duration::from_millis(50),
			batch_size: 1024,
		}
	}

	pub fn bind_addr(mut self, addr: impl Into<String>) -> Self {
		self.bind_addr = Some(addr.into());
		self
	}

	pub fn poll_interval(mut self, interval: Duration) -> Self {
		self.poll_interval = interval;
		self
	}

	pub fn batch_size(mut self, size: u64) -> Self {
		self.batch_size = size;
		self
	}

	pub(crate) fn configure(self) -> PrimaryConfig {
		PrimaryConfig {
			bind_addr: self.bind_addr,
			poll_interval: self.poll_interval,
			batch_size: self.batch_size,
		}
	}
}

impl From<PrimaryConfigurator> for ReplicationConfig {
	fn from(c: PrimaryConfigurator) -> Self {
		ReplicationConfig::Primary(c.configure())
	}
}

pub struct ReplicaConfigurator {
	primary_addr: Option<String>,
	batch_size: u64,
	reconnect_interval: Duration,
}

impl Default for ReplicaConfigurator {
	fn default() -> Self {
		Self::new()
	}
}

impl ReplicaConfigurator {
	pub fn new() -> Self {
		Self {
			primary_addr: None,
			batch_size: 1024,
			reconnect_interval: Duration::from_secs(1),
		}
	}

	pub fn primary_addr(mut self, addr: impl Into<String>) -> Self {
		self.primary_addr = Some(addr.into());
		self
	}

	pub fn batch_size(mut self, size: u64) -> Self {
		self.batch_size = size;
		self
	}

	pub fn reconnect_interval(mut self, interval: Duration) -> Self {
		self.reconnect_interval = interval;
		self
	}

	pub(crate) fn configure(self) -> ReplicaConfig {
		ReplicaConfig {
			primary_addr: self.primary_addr,
			batch_size: self.batch_size,
			reconnect_interval: self.reconnect_interval,
		}
	}
}

impl From<ReplicaConfigurator> for ReplicationConfig {
	fn from(c: ReplicaConfigurator) -> Self {
		ReplicationConfig::Replica(c.configure())
	}
}

pub struct PrimaryConfig {
	pub bind_addr: Option<String>,
	pub poll_interval: Duration,
	pub batch_size: u64,
}

pub struct ReplicaConfig {
	pub primary_addr: Option<String>,
	pub batch_size: u64,
	pub reconnect_interval: Duration,
}

pub enum ReplicationConfig {
	Primary(PrimaryConfig),
	Replica(ReplicaConfig),
}

pub struct ReplicationConfigurator;

impl ReplicationConfigurator {
	pub fn primary(self) -> PrimaryConfigurator {
		PrimaryConfigurator::new()
	}

	pub fn replica(self) -> ReplicaConfigurator {
		ReplicaConfigurator::new()
	}
}
