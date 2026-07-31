// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_cdc::consume::{checkpoint::CdcCheckpoint, watermark::compute_pinning_watermark};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::CdcConsumerId,
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

	pub fn flow_watermark(&self) -> CommitVersion {
		let mut txn = match self.begin_query(IdentityId::system()) {
			Ok(txn) => txn,
			Err(_) => return CommitVersion(0),
		};
		match CdcCheckpoint::fetch_opt(&mut Transaction::Query(&mut txn), &CdcConsumerId::flow_consumer()) {
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

	// The historical-GC cutoff is bounded by held leases only, never by a long-lived consumer
	// watermark: an in-flight subscription batch protects its reads through its lease, and a
	// lagging consumer with no lease must NOT lower the cutoff (that unbounded pin is exactly the
	// slow-consumer stall the consumer-class split removed). Releasing the lease releases the pin.
	#[test]
	fn effective_gc_cutoff_is_lowered_by_a_held_lease_and_nothing_else() {
		let t = TestEngine::new();

		// Lease the pre-advance head first: acquiring after the watermark moved past it would be
		// rejected as evicted (TXN_012), which is the overtaken signal, not the pin under test.
		let lagging = CommitVersion(50);
		let lease = t.multi().acquire_version_lease(lagging).expect("leasing at the current head must succeed");

		// Advance the query watermark to a known positive baseline. A bare engine sits at version 0,
		// so without this the cutoff would be 0 and there would be nothing to lower below.
		t.multi().advance_version_to(CommitVersion(100));

		assert_eq!(
			t.effective_gc_cutoff(),
			lagging,
			"a held lease must lower the historical-GC cutoff to the leased version"
		);

		// Releasing the lease releases the pin: the cutoff returns to the query watermark. If a
		// consumer-watermark term still existed, a lagging consumer position would keep the cutoff
		// down here and history would be pinned without bound.
		drop(lease);
		assert!(
			t.effective_gc_cutoff().0 >= 100,
			"with no lease held, nothing may pin the cutoff below the query watermark"
		);
	}
}
