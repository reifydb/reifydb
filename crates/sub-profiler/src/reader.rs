// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Read-only handle over the `ProfileAccumulator`. Returned by `ProfilerSubsystem::reader()`. Cheap to clone (it's
//! just an `Arc<RwLock<_>>`). Reads see whatever the actor has folded so far; the accumulator itself is transient
//! and is reset/drained by the metric subsystem on its own cadence.

use std::sync::Arc;

use parking_lot::RwLock;
use reifydb_profiler::{category::ProfileCategory, record::AggregateRecord};

use crate::accumulator::ProfileAccumulator;

#[derive(Clone)]
pub struct ProfilerReader {
	accumulator: Arc<RwLock<ProfileAccumulator>>,
}

impl ProfilerReader {
	pub fn new(accumulator: Arc<RwLock<ProfileAccumulator>>) -> Self {
		Self {
			accumulator,
		}
	}

	pub fn top_n(&self, category: ProfileCategory, n: usize) -> Vec<AggregateRecord> {
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
