// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_cdc::consume::watermark::compute_pinning_watermark;
use reifydb_core::{
	common::CommitVersion,
	lifecycle::watermark::{EvictionWatermark, QueryWatermark},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::identity::IdentityId;

use crate::engine::StandardEngine;

impl QueryWatermark for StandardEngine {
	fn effective_gc_cutoff(&self) -> CommitVersion {
		let qdu = self.query_done_until();
		let lease_min = self.multi().leases().min_active().unwrap_or(CommitVersion(u64::MAX));
		qdu.min(lease_min)
	}
}

impl EvictionWatermark for StandardEngine {
	fn watermark(&self) -> CommitVersion {
		self.effective_gc_cutoff().min(self.consumer_watermark())
	}
}

impl StandardEngine {
	pub fn consumer_watermark(&self) -> CommitVersion {
		let mut txn = match self.begin_query(IdentityId::system()) {
			Ok(txn) => txn,
			Err(_) => return CommitVersion(0),
		};
		match compute_pinning_watermark(&mut Transaction::Query(&mut txn)) {
			Ok(Some(v)) => v,
			Ok(None) => CommitVersion(u64::MAX),
			Err(_) => CommitVersion(0),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{common::CommitVersion, lifecycle::watermark::QueryWatermark};

	use crate::test_harness::TestEngine;

	#[test]
	fn effective_gc_cutoff_is_lowered_by_a_held_lease_and_nothing_else() {
		// Only a held lease may pin the cutoff. A lagging consumer without one pinning it is
		// an unbounded stall, so the pin has to end when the lease is dropped.
		let t = TestEngine::new();

		// Leased before the advance; acquiring after would be rejected as evicted, which is the
		// overtaken signal rather than the pin under test.
		let lagging = CommitVersion(50);
		let lease = t.multi().acquire_version_lease(lagging).expect("leasing at the current head must succeed");

		// A bare engine sits at version 0, so without a positive baseline there is nothing the
		// lease could lower the cutoff below.
		t.multi().advance_version_to(CommitVersion(100));

		assert_eq!(
			t.effective_gc_cutoff(),
			lagging,
			"a held lease must lower the historical-GC cutoff to the leased version"
		);

		drop(lease);
		assert!(
			t.effective_gc_cutoff().0 >= 100,
			"with no lease held, nothing may pin the cutoff below the query watermark"
		);
	}
}
