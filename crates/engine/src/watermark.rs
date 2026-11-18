// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, lifecycle::watermark::QueryWatermark};

use crate::engine::StandardEngine;

impl QueryWatermark for StandardEngine {
	fn effective_gc_cutoff(&self) -> CommitVersion {
		let qdu = self.query_done_until();
		let lease_min = self.multi().leases().min_active().unwrap_or(CommitVersion(u64::MAX));
		qdu.min(lease_min)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{
			id::{IndexId, TableId},
			object::ObjectId,
		},
		key::catalog::IndexEntryKey,
		lifecycle::watermark::QueryWatermark,
		value::index::encoded::EncodedIndexKey,
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::util::cowvec::CowVec;

	#[test]
	fn effective_gc_cutoff_is_lowered_by_a_held_lease_and_nothing_else() {
		// Only a held lease may pin the cutoff. A lagging consumer without one pinning it is
		// an unbounded stall, so the pin has to end when the lease is dropped.
		let t = TestEngine::new();

		// Leased before the advance; acquiring after would be rejected as evicted, which is the
		// overtaken signal rather than the pin under test.
		let lagging = CommitVersion(t.query_done_until().0 + 4);
		let lease = t.multi().acquire_version_lease(lagging).expect("leasing at the current head must succeed");

		// A bare engine sits at version 0, so without a positive baseline there is nothing the
		// lease could lower the cutoff below.
		while t.query_done_until().0 < lagging.0 + 4 {
			let mut bump = t.multi().begin_command(t.multi().generate_transaction_id()).unwrap();
			let key = IndexEntryKey::new(
				ObjectId::Table(TableId(1)),
				IndexId::primary(1u64),
				EncodedIndexKey::new([1u8]),
			);
			bump.set(vec![(key.into(), EncodedBytes(CowVec::new(vec![1])))]).unwrap();
			bump.commit().unwrap();
		}

		assert_eq!(
			t.effective_gc_cutoff(),
			lagging,
			"a held lease must lower the historical-GC cutoff to the leased version"
		);

		drop(lease);
		assert!(
			t.effective_gc_cutoff().0 >= lagging.0 + 4,
			"with no lease held, nothing may pin the cutoff below the query watermark"
		);
	}
}
