// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_profiler::{category::ProfilerCategory, record::AggregateRecord};
use reifydb_runtime::sync::rwlock::RwLock;

use crate::accumulator::ProfilerAccumulator;

#[derive(Clone)]
pub struct ProfilerReader {
	accumulator: Arc<RwLock<ProfilerAccumulator>>,
}

impl ProfilerReader {
	pub fn new(accumulator: Arc<RwLock<ProfilerAccumulator>>) -> Self {
		Self {
			accumulator,
		}
	}

	pub fn top_n(&self, category: ProfilerCategory, n: usize) -> Vec<AggregateRecord> {
		self.accumulator.read().top_n(category, n)
	}

	pub fn all(&self) -> Vec<AggregateRecord> {
		self.accumulator.read().all()
	}

	pub fn len(&self) -> usize {
		self.accumulator.read().len()
	}

	pub fn is_empty(&self) -> bool {
		self.accumulator.read().is_empty()
	}
}
