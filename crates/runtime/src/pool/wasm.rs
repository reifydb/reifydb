// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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

	pub fn shutdown(&self) {}

	pub fn spawn_task(&self, job: impl FnOnce() + Send + 'static) {
		job();
	}

	pub fn task_thread_count(&self) -> usize {
		1
	}

	pub fn compute(&self) -> StubPool {
		StubPool
	}

	pub fn compute_thread_count(&self) -> usize {
		1
	}

	pub fn coordination_thread_count(&self) -> usize {
		1
	}

	pub fn flow_thread_count(&self) -> usize {
		1
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
