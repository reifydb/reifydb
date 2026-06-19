// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_cdc::consume::watermark::compute_watermark;
use reifydb_core::common::CommitVersion;
use reifydb_store_multi::gc::{EvictionWatermark, historical::QueryWatermark};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::identity::IdentityId;

use crate::engine::StandardEngine;

impl QueryWatermark for StandardEngine {
	fn effective_gc_cutoff(&self) -> CommitVersion {
		let qdu = self.query_done_until();
		let lease_min = self.multi().leases().min_active().unwrap_or(CommitVersion(u64::MAX));
		qdu.min(lease_min).min(self.multi().consumer_watermark())
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
		match compute_watermark(&mut Transaction::Query(&mut txn)) {
			Ok(Some(v)) => v,
			Ok(None) => CommitVersion(u64::MAX),
			Err(_) => CommitVersion(0),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::common::CommitVersion;
	use reifydb_store_multi::gc::historical::QueryWatermark;

	use crate::test_harness::TestEngine;

	// The historical-GC cutoff must be lowered by the CDC consumer watermark so the version store
	// retains snapshots a lagging consumer (e.g. a subscription worker draining a backlog) still
	// needs to read. This is the storage half of the TXN_012 fix; the lease-acquire half is covered
	// in the transaction crate's write tests.
	#[test]
	fn effective_gc_cutoff_is_lowered_by_the_consumer_watermark() {
		let t = TestEngine::new();

		// Advance the query watermark to a known positive baseline. A bare engine sits at version 0,
		// so without this the cutoff would be 0 and there would be nothing to lower below.
		t.multi().advance_version_to(CommitVersion(100));

		// Nothing pins the consumer term by default (u64::MAX), so the cutoff is just the query/lease
		// watermark.
		let baseline = t.effective_gc_cutoff();
		assert!(baseline.0 >= 100, "precondition: the query watermark is advanced to the baseline");

		// A consumer lagging just below the baseline must pull the cutoff down to its own position so
		// the version it has not consumed yet is retained.
		let lagging = CommitVersion(baseline.0 - 1);
		t.multi().set_consumer_watermark(lagging);
		assert_eq!(
			t.effective_gc_cutoff(),
			lagging,
			"the consumer watermark must lower the historical-GC cutoff to the consumer position"
		);

		// Restoring the term to u64::MAX makes it inert: the cutoff returns to the query watermark.
		// This guards against a 0 default, which would have pinned all history.
		t.multi().set_consumer_watermark(CommitVersion(u64::MAX));
		assert!(
			t.effective_gc_cutoff().0 >= baseline.0,
			"a u64::MAX consumer watermark must not pin the cutoff below the query watermark"
		);
	}
}
