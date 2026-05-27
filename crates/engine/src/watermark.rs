// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_cdc::consume::watermark::compute_watermark;
use reifydb_core::common::CommitVersion;
use reifydb_store_multi::gc::{EvictionWatermark, historical::QueryWatermark};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::identity::IdentityId;

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
	fn consumer_watermark(&self) -> CommitVersion {
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
