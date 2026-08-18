// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::from_bytes;
use reifydb_core::interface::change::Change;
use reifydb_sdk::flow::operator::timer::Timer as Tick;
use reifydb_value::{Result, error::Error, util::hash::Hash128, value::duration::Duration};

use crate::flow::{
	error::FlowStateError,
	operator::{
		distinct::{operator::DistinctOperator, state::DistinctEntry},
		stateful::utils,
	},
	transaction::FlowTransaction,
};

impl DistinctOperator {
	pub(super) fn ticks_interval(&self) -> Option<Duration> {
		if self.ttl_nanos.is_some() {
			Some(Duration::from_seconds(1).unwrap())
		} else {
			None
		}
	}

	pub(super) fn tick_evict(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		let Some(ttl_nanos) = self.ttl_nanos else {
			return Ok(None);
		};
		let cutoff = tick.due.to_nanos().saturating_sub(ttl_nanos);

		let mut expired: Vec<Hash128> = Vec::new();
		for (key, row) in utils::state_scan_all(self.node, txn)? {
			let Some(hash) = Self::hash_from_entry_key(key.as_ref()) else {
				continue;
			};
			let blob = self.shape.get_blob(row.bytes(), 0);
			if blob.is_empty() {
				continue;
			}
			let entry: DistinctEntry = from_bytes(blob.as_ref()).map_err(|e| {
				Error::from(FlowStateError::Decode {
					state: "DistinctEntry",
					cause: e.to_string(),
				})
			})?;
			if entry.last_seen_nanos < cutoff {
				expired.push(hash);
			}
		}

		for hash in expired {
			utils::state_drop(self.node, txn, &Self::entry_key(hash))?;
		}

		Ok(None)
	}
}
