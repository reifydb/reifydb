// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_value::count::Count;

#[derive(Default)]
pub struct EpochGauge {
	durable_samples: AtomicU64,
	pruned: AtomicU64,
}

impl EpochGauge {
	pub fn record(&self, durable_samples: Count, pruned: Count) {
		self.durable_samples.store(durable_samples.as_u64(), Ordering::Relaxed);
		self.pruned.store(pruned.as_u64(), Ordering::Relaxed);
	}

	pub fn read(&self) -> (u64, u64) {
		(self.durable_samples.load(Ordering::Relaxed), self.pruned.load(Ordering::Relaxed))
	}
}
