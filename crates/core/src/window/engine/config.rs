// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::byte_size::ByteSize;

use crate::{state::budget::OperatorStateBudgetHandle, window::span::Slot};

pub const DEFAULT_OPERATOR_STATE_BUDGET: ByteSize = ByteSize::from_bytes(2 * 1024 * 1024 * 1024);

pub const DEFAULT_EXPIRE_BATCH: usize = 256;

#[derive(Clone)]
pub struct WindowEngineConfig {
	budget: OperatorStateBudgetHandle,
	expire_batch: usize,
}

impl WindowEngineConfig {
	pub fn builder() -> WindowEngineConfigBuilder {
		WindowEngineConfigBuilder::default()
	}

	pub fn budget(&self) -> OperatorStateBudgetHandle {
		self.budget.clone()
	}

	pub fn expire_batch(&self) -> usize {
		self.expire_batch
	}
}

pub struct WindowEngineConfigBuilder {
	budget: Option<OperatorStateBudgetHandle>,
	expire_batch: usize,
}

impl Default for WindowEngineConfigBuilder {
	fn default() -> Self {
		Self {
			budget: None,
			expire_batch: DEFAULT_EXPIRE_BATCH,
		}
	}
}

impl WindowEngineConfigBuilder {
	pub fn budget(mut self, budget: OperatorStateBudgetHandle) -> Self {
		self.budget = Some(budget);
		self
	}

	pub fn expire_batch(mut self, batch: usize) -> Self {
		self.expire_batch = batch;
		self
	}

	pub fn build(self) -> WindowEngineConfig {
		WindowEngineConfig {
			budget: self.budget.unwrap_or_default(),
			expire_batch: self.expire_batch,
		}
	}
}

pub struct TumblingCarryConfig<C: Slot> {
	base: WindowEngineConfig,
	retention: Option<C::Duration>,
}

impl<C: Slot> TumblingCarryConfig<C> {
	pub fn builder() -> TumblingCarryConfigBuilder<C> {
		TumblingCarryConfigBuilder::new()
	}

	pub fn base(&self) -> WindowEngineConfig {
		self.base.clone()
	}

	pub fn retention(&self) -> Option<C::Duration> {
		self.retention
	}
}

pub struct TumblingCarryConfigBuilder<C: Slot> {
	base: WindowEngineConfig,
	retention: Option<C::Duration>,
}

impl<C: Slot> TumblingCarryConfigBuilder<C> {
	fn new() -> Self {
		Self {
			base: WindowEngineConfig::builder().build(),
			retention: None,
		}
	}

	pub fn base(mut self, base: WindowEngineConfig) -> Self {
		self.base = base;
		self
	}

	pub fn retention(mut self, retention: Option<C::Duration>) -> Self {
		self.retention = retention;
		self
	}

	pub fn build(self) -> TumblingCarryConfig<C> {
		TumblingCarryConfig {
			base: self.base,
			retention: self.retention,
		}
	}
}
