// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_core::util::ioc::IocContainer;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::SharedRuntime;
use reifydb_sub_api::subsystem::{Subsystem, SubsystemFactory};
use reifydb_sub_server::state::{AppState, StateConfig};
use reifydb_type::Result;

use crate::subsystem::GrpcSubsystem;

#[derive(Clone, Debug)]
pub struct GrpcConfig {
	pub bind_addr: String,
	pub max_connections: usize,
	pub query_timeout: Duration,
	pub request_timeout: Duration,
	pub runtime: Option<SharedRuntime>,
	pub poll_interval: Duration,
	pub poll_batch_size: usize,
}

impl Default for GrpcConfig {
	fn default() -> Self {
		Self {
			bind_addr: "0.0.0.0:50051".to_string(),
			max_connections: 10_000,
			query_timeout: Duration::from_secs(30),
			request_timeout: Duration::from_secs(60),
			runtime: None,
			poll_interval: Duration::from_millis(10),
			poll_batch_size: 100,
		}
	}
}

impl GrpcConfig {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn bind_addr(mut self, addr: impl Into<String>) -> Self {
		self.bind_addr = addr.into();
		self
	}

	pub fn max_connections(mut self, max: usize) -> Self {
		self.max_connections = max;
		self
	}

	pub fn query_timeout(mut self, timeout: Duration) -> Self {
		self.query_timeout = timeout;
		self
	}

	pub fn request_timeout(mut self, timeout: Duration) -> Self {
		self.request_timeout = timeout;
		self
	}

	pub fn runtime(mut self, runtime: SharedRuntime) -> Self {
		self.runtime = Some(runtime);
		self
	}

	pub fn poll_interval(mut self, interval: Duration) -> Self {
		self.poll_interval = interval;
		self
	}

	pub fn poll_batch_size(mut self, size: usize) -> Self {
		self.poll_batch_size = size;
		self
	}
}

pub struct GrpcSubsystemFactory {
	config: GrpcConfig,
}

impl GrpcSubsystemFactory {
	pub fn new(config: GrpcConfig) -> Self {
		Self {
			config,
		}
	}
}

impl SubsystemFactory for GrpcSubsystemFactory {
	fn create(self: Box<Self>, ioc: &IocContainer) -> Result<Box<dyn Subsystem>> {
		let engine = ioc.resolve::<StandardEngine>()?;
		let ioc_runtime = ioc.resolve::<SharedRuntime>()?;

		let query_config = StateConfig::new()
			.query_timeout(self.config.query_timeout)
			.request_timeout(self.config.request_timeout)
			.max_connections(self.config.max_connections);

		let runtime = self.config.runtime.unwrap_or(ioc_runtime);

		let state = AppState::new(runtime.actor_system(), engine, query_config);
		let subsystem = GrpcSubsystem::new(
			self.config.bind_addr.clone(),
			state,
			runtime,
			self.config.poll_interval,
			self.config.poll_batch_size,
		);

		Ok(Box::new(subsystem))
	}
}
