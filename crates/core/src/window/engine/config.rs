// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::window::{engine::LatePolicy, span::Slot};

pub const DEFAULT_STATE_CACHE_CAPACITY: usize = 1024;

pub const DEFAULT_INTERNAL_STATE_CACHE_CAPACITY: usize = 1024;

#[derive(Clone, Copy, Debug)]
pub struct WindowEngineConfig {
	late_policy: LatePolicy,
	state_cache_capacity: usize,
	internal_state_cache_capacity: usize,
}

impl WindowEngineConfig {
	pub fn builder() -> WindowEngineConfigBuilder {
		WindowEngineConfigBuilder::default()
	}

	pub fn late_policy(&self) -> LatePolicy {
		self.late_policy
	}

	pub fn state_cache_capacity(&self) -> usize {
		self.state_cache_capacity
	}

	pub fn internal_state_cache_capacity(&self) -> usize {
		self.internal_state_cache_capacity
	}
}

#[derive(Clone, Copy, Debug)]
pub struct WindowEngineConfigBuilder {
	late_policy: LatePolicy,
	state_cache_capacity: usize,
	internal_state_cache_capacity: usize,
}

impl Default for WindowEngineConfigBuilder {
	fn default() -> Self {
		Self {
			late_policy: LatePolicy::Drop,
			state_cache_capacity: DEFAULT_STATE_CACHE_CAPACITY,
			internal_state_cache_capacity: DEFAULT_INTERNAL_STATE_CACHE_CAPACITY,
		}
	}
}

impl WindowEngineConfigBuilder {
	pub fn late_policy(mut self, late_policy: LatePolicy) -> Self {
		self.late_policy = late_policy;
		self
	}

	pub fn state_cache_capacity(mut self, capacity: usize) -> Self {
		self.state_cache_capacity = capacity;
		self
	}

	pub fn internal_state_cache_capacity(mut self, capacity: usize) -> Self {
		self.internal_state_cache_capacity = capacity;
		self
	}

	pub fn build(self) -> WindowEngineConfig {
		WindowEngineConfig {
			late_policy: self.late_policy,
			state_cache_capacity: self.state_cache_capacity,
			internal_state_cache_capacity: self.internal_state_cache_capacity,
		}
	}
}

#[derive(Clone, Copy, Debug)]
pub struct TumblingCarryConfig<C: Slot> {
	base: WindowEngineConfig,
	retention: Option<C::Duration>,
}

impl<C: Slot> TumblingCarryConfig<C> {
	pub fn builder() -> TumblingCarryConfigBuilder<C> {
		TumblingCarryConfigBuilder::new()
	}

	pub fn base(&self) -> WindowEngineConfig {
		self.base
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
