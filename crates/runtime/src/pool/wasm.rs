// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::PoolConfig;

#[derive(Clone)]
pub struct Pools;

impl Default for Pools {
	fn default() -> Self {
		Self
	}
}

impl Pools {
	pub fn new(_config: PoolConfig) -> Self {
		Self
	}

	pub fn system_pool(&self) -> StubPool {
		StubPool
	}

	pub fn system_thread_count(&self) -> usize {
		1
	}

	pub fn query_pool(&self) -> StubPool {
		StubPool
	}
}

#[derive(Clone)]
pub struct StubPool;

impl StubPool {
	pub fn install<F, R>(&self, f: F) -> R
	where
		F: FnOnce() -> R,
	{
		f()
	}
}
