// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::operator::state::OperatorStateKey;
use rusqlite::{Transaction, TransactionBehavior, params};
use tracing::instrument;

use crate::{
	persistent::sqlite::{
		SqlitePersistent, route,
		sql::{CHECKPOINT_REMOVE_SQL, CHECKPOINT_SET_SQL},
	},
	resident::bucket::BucketMap,
	types::{DropMarker, FlushBatch, StagedWrite},
};

impl SqlitePersistent {
	#[instrument(name = "store::operator::persistent::sqlite::flush_batch", level = "debug", skip(self, batch), fields(
		drop_count = batch.drops.len(),
		state_count = batch.writes.len(),
		checkpoint_count = batch.checkpoints.len()
	))]
	pub fn flush_batch(&self, batch: &FlushBatch) {
		if !batch.writes.is_empty() {
			self.mark_state_written();
		}
		let guard = self.inner.conn.lock();
		let conn = guard.as_ref().expect("operator state flush ran without an open connection");
		let transaction = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
			.expect("operator state flush could not begin");
		for marker in &batch.drops {
			match marker {
				DropMarker::OperatorState(operator) => {
					route::drop_operator(&transaction, *operator);
				}
			}
		}

		state_of(batch).write_into(&transaction);

		for (flow, entry) in &batch.checkpoints {
			match entry {
				Some(version) => transaction
					.prepare_cached(CHECKPOINT_SET_SQL)
					.expect("flow checkpoint write could not be prepared")
					.execute(params![flow.0 as i64, version.0 as i64])
					.expect("flow checkpoint write failed"),
				None => transaction
					.prepare_cached(CHECKPOINT_REMOVE_SQL)
					.expect("flow checkpoint delete could not be prepared")
					.execute(params![flow.0 as i64])
					.expect("flow checkpoint delete failed"),
			};
		}

		transaction.commit().expect("operator state flush could not commit");
	}
}

fn state_of(batch: &FlushBatch) -> BucketMap {
	let mut state = BucketMap::default();
	for (operator, key, write) in &batch.writes {
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())
			.expect("a staged operator state key must decode as one");
		let post = match write {
			StagedWrite::Set(row) => Some(row.clone()),
			StagedWrite::Remove => None,
		};
		state.record_bytes(*operator, keyspace, group, suffix, post);
	}
	state
}
