// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::state::budget::OperatorStateBudgetHandle;

use crate::window::span::{SlotSpan, WindowAnchor};

pub const DEFAULT_EXPIRE_BATCH: usize = 256;

#[derive(Clone)]
pub struct WindowEngineConfig {
	budget: OperatorStateBudgetHandle,
	expire_batch: usize,
}

impl WindowEngineConfig {
	pub fn builder(budget: OperatorStateBudgetHandle) -> WindowEngineConfigBuilder {
		WindowEngineConfigBuilder::new(budget)
	}

	pub fn budget(&self) -> OperatorStateBudgetHandle {
		self.budget.clone()
	}

	pub fn expire_batch(&self) -> usize {
		self.expire_batch
	}
}

pub struct WindowEngineConfigBuilder {
	budget: OperatorStateBudgetHandle,
	expire_batch: usize,
}

impl WindowEngineConfigBuilder {
	fn new(budget: OperatorStateBudgetHandle) -> Self {
		Self {
			budget,
			expire_batch: DEFAULT_EXPIRE_BATCH,
		}
	}

	pub fn expire_batch(mut self, batch: usize) -> Self {
		self.expire_batch = batch;
		self
	}

	pub fn build(self) -> WindowEngineConfig {
		WindowEngineConfig {
			budget: self.budget,
			expire_batch: self.expire_batch,
		}
	}
}

pub struct TumblingCarryConfig<C: WindowAnchor> {
	base: WindowEngineConfig,
	retention: Option<SlotSpan<C>>,
}

impl<C: WindowAnchor> TumblingCarryConfig<C> {
	pub fn builder(base: WindowEngineConfig) -> TumblingCarryConfigBuilder<C> {
		TumblingCarryConfigBuilder::new(base)
	}

	pub fn base(&self) -> WindowEngineConfig {
		self.base.clone()
	}

	pub fn retention(&self) -> Option<SlotSpan<C>> {
		self.retention
	}
}

pub struct TumblingCarryConfigBuilder<C: WindowAnchor> {
	base: WindowEngineConfig,
	retention: Option<SlotSpan<C>>,
}

impl<C: WindowAnchor> TumblingCarryConfigBuilder<C> {
	fn new(base: WindowEngineConfig) -> Self {
		Self {
			base,
			retention: None,
		}
	}

	pub fn retention(mut self, retention: Option<SlotSpan<C>>) -> Self {
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

#[cfg(test)]
mod tests {
	use reifydb_value::{byte_size::ByteSize, value::datetime::DateTime};

	use super::*;

	#[test]
	fn a_config_shares_the_pool_it_was_given_instead_of_detaching_a_copy() {
		// Every cache the engine owns charges through config.budget(). A fresh pool rather than the
		// caller's would enforce a private ceiling and keep those bytes out of the host accounting.
		let pool = OperatorStateBudgetHandle::new(ByteSize::from_bytes(4096));
		let config = WindowEngineConfig::builder(pool.clone()).build();

		config.budget().charge_clean(ByteSize::from_bytes(64));

		assert_eq!(
			pool.snapshot().resident,
			ByteSize::from_bytes(64),
			"a charge through the config must land in the pool the caller passed in"
		);
	}

	#[test]
	fn a_carry_config_forwards_the_pool_through_its_base() {
		let pool = OperatorStateBudgetHandle::new(ByteSize::from_bytes(4096));
		let config: TumblingCarryConfig<DateTime> =
			TumblingCarryConfig::builder(WindowEngineConfig::builder(pool.clone()).build())
				.retention(None)
				.build();

		config.base().budget().charge_clean(ByteSize::from_bytes(32));

		assert_eq!(pool.snapshot().resident, ByteSize::from_bytes(32));
	}
}
