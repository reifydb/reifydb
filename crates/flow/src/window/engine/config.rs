// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::window::span::{SlotSpan, WindowAnchor};

pub const DEFAULT_EXPIRE_BATCH: usize = 256;

#[derive(Clone)]
pub struct WindowEngineConfig {
	expire_batch: usize,
}

impl WindowEngineConfig {
	pub fn builder() -> WindowEngineConfigBuilder {
		WindowEngineConfigBuilder::new()
	}

	pub fn expire_batch(&self) -> usize {
		self.expire_batch
	}
}

pub struct WindowEngineConfigBuilder {
	expire_batch: usize,
}

impl WindowEngineConfigBuilder {
	fn new() -> Self {
		Self {
			expire_batch: DEFAULT_EXPIRE_BATCH,
		}
	}

	pub fn expire_batch(mut self, batch: usize) -> Self {
		self.expire_batch = batch;
		self
	}

	pub fn build(self) -> WindowEngineConfig {
		WindowEngineConfig {
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
	use reifydb_value::value::datetime::DateTime;

	use super::*;

	#[test]
	fn the_expire_batch_defaults_and_survives_an_override() {
		// The batch bounds one expiry pass; a builder that dropped it would sweep unbounded.
		assert_eq!(WindowEngineConfig::builder().build().expire_batch(), DEFAULT_EXPIRE_BATCH);
		assert_eq!(WindowEngineConfig::builder().expire_batch(9).build().expire_batch(), 9);
	}

	#[test]
	fn a_carry_config_forwards_its_base() {
		let config: TumblingCarryConfig<DateTime> =
			TumblingCarryConfig::builder(WindowEngineConfig::builder().expire_batch(7).build())
				.retention(None)
				.build();

		assert_eq!(config.base().expire_batch(), 7, "the carry config must not detach a fresh base");
	}
}
