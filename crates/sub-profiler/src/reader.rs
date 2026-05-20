// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
