// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_value::count::Count;

/// What only the durable epoch log can see about itself.
///
/// Fed from the event bus: the log belongs to the lifecycle subsystem, and reaching into it from a metrics domain
/// would couple two subsystems that otherwise share nothing but core.
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
