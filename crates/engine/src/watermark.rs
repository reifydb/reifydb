// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_cdc::consume::watermark::compute_pinning_watermark;
use reifydb_core::{
	common::CommitVersion,
	lifecycle::watermark::{EvictionWatermark, QueryWatermark},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::identity::IdentityId;
use tracing::error;

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
			Err(err) => {
				error!(error = %err, "cannot begin a query for the pinning watermark, holding cdc retention at version zero");
				return CommitVersion(0);
			}
		};
		match compute_pinning_watermark(&mut Transaction::Query(&mut txn), Some(&*self.checkpoint_floor())) {
			Ok(Some(v)) => v,
			Ok(None) => CommitVersion(u64::MAX),
			Err(err) => {
				error!(error = %err, "pinning watermark unavailable, holding cdc retention at version zero");
				CommitVersion(0)
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{common::CommitVersion, lifecycle::watermark::QueryWatermark};
	use reifydb_runtime::shutdown::Shutdown;
	use reifydb_test_harness::engine::TestEngine;

	#[test]
	fn a_closed_operator_store_holds_the_consumer_watermark_at_zero_instead_of_releasing_retention() {
		// an unreadable floor must never read as "nothing to pin". u64::MAX is the no-pin answer and
		// it lets cdc reap every version the flows still need, so a closed store has to invert it to 0.
		let t = TestEngine::new();

		assert_eq!(
			t.inner().consumer_watermark(),
			CommitVersion(u64::MAX),
			"a healthy store with no checkpoint rows pins nothing, which is what makes the closed case \
			 below a real inversion rather than a no-op"
		);

		t.inner().operator_state().shutdown();

		assert_eq!(
			t.inner().consumer_watermark(),
			CommitVersion(0),
			"once the floor cannot be read the engine must hold retention at zero; answering u64::MAX \
			 here releases every cdc entry the flows have not replayed yet"
		);
	}

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
