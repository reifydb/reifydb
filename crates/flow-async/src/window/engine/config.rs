// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::window::{
	engine::KeyspaceFamily,
	span::{SlotSpan, WindowAnchor},
};

pub const DEFAULT_DUE_BATCH: usize = 256;

#[derive(Clone)]
pub struct WindowEngineConfig {
	due_batch: usize,
	family: KeyspaceFamily,
}

impl WindowEngineConfig {
	pub fn builder() -> WindowEngineConfigBuilder {
		WindowEngineConfigBuilder::new()
	}

	pub fn due_batch(&self) -> usize {
		self.due_batch
	}

	pub fn family(&self) -> KeyspaceFamily {
		self.family
	}
}

pub struct WindowEngineConfigBuilder {
	due_batch: usize,
	family: KeyspaceFamily,
}

impl WindowEngineConfigBuilder {
	fn new() -> Self {
		Self {
			due_batch: DEFAULT_DUE_BATCH,
			family: KeyspaceFamily::Host,
		}
	}

	pub fn due_batch(mut self, batch: usize) -> Self {
		self.due_batch = batch;
		self
	}

	pub fn family(mut self, family: KeyspaceFamily) -> Self {
		self.family = family;
		self
	}

	pub fn build(self) -> WindowEngineConfig {
		WindowEngineConfig {
			due_batch: self.due_batch,
			family: self.family,
		}
	}
}

pub struct TumblingCarryConfig<S: WindowAnchor> {
	base: WindowEngineConfig,
	immutable: Option<SlotSpan<S>>,
	idle_retention: Option<SlotSpan<S>>,
}

impl<S: WindowAnchor> TumblingCarryConfig<S> {
	pub fn builder(base: WindowEngineConfig) -> TumblingCarryConfigBuilder<S> {
		TumblingCarryConfigBuilder::new(base)
	}

	pub fn base(&self) -> WindowEngineConfig {
		self.base.clone()
	}

	pub fn immutable(&self) -> Option<SlotSpan<S>> {
		self.immutable
	}

	pub fn idle_retention(&self) -> Option<SlotSpan<S>> {
		self.idle_retention
	}
}

pub struct TumblingCarryConfigBuilder<S: WindowAnchor> {
	base: WindowEngineConfig,
	immutable: Option<SlotSpan<S>>,
	idle_retention: Option<SlotSpan<S>>,
}

impl<S: WindowAnchor> TumblingCarryConfigBuilder<S> {
	fn new(base: WindowEngineConfig) -> Self {
		Self {
			base,
			immutable: None,
			idle_retention: None,
		}
	}

	pub fn immutable(mut self, immutable: Option<SlotSpan<S>>) -> Self {
		self.immutable = immutable;
		self
	}

	pub fn idle_retention(mut self, idle_retention: Option<SlotSpan<S>>) -> Self {
		self.idle_retention = idle_retention;
		self
	}

	pub fn build(self) -> TumblingCarryConfig<S> {
		TumblingCarryConfig {
			base: self.base,
			immutable: self.immutable,
			idle_retention: self.idle_retention,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::window::coord::ByDateTime;

	#[test]
	fn the_due_batch_defaults_and_survives_an_override() {
		// The batch bounds one pass over the due index; a builder that dropped it would sweep unbounded.
		assert_eq!(WindowEngineConfig::builder().build().due_batch(), DEFAULT_DUE_BATCH);
		assert_eq!(WindowEngineConfig::builder().due_batch(9).build().due_batch(), 9);
	}

	#[test]
	fn a_carry_config_forwards_its_base() {
		let config: TumblingCarryConfig<ByDateTime> =
			TumblingCarryConfig::builder(WindowEngineConfig::builder().due_batch(7).build())
				.immutable(None)
				.build();

		assert_eq!(config.base().due_batch(), 7, "the carry config must not detach a fresh base");
	}
}
