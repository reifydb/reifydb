// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use rayon::{ThreadPool, ThreadPoolBuilder};

pub struct ComputePool {
	pool: Arc<ThreadPool>,
}

impl ComputePool {
	pub(crate) fn new(threads: usize, name_prefix: &'static str) -> Self {
		Self {
			pool: Arc::new(
				ThreadPoolBuilder::new()
					.num_threads(threads)
					.thread_name(move |i| format!("{name_prefix}-{i}"))
					.build()
					.unwrap_or_else(|_| panic!("failed to build {name_prefix} thread pool")),
			),
		}
	}

	pub fn install<OP, R>(&self, op: OP) -> R
	where
		OP: FnOnce() -> R + Send,
		R: Send,
	{
		self.pool.install(op)
	}

	pub fn thread_count(&self) -> usize {
		self.pool.current_num_threads()
	}
}
