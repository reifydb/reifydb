// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rusqlite::{Transaction, TransactionBehavior, params};
use tracing::instrument;

use crate::tier::{
	persistent::sqlite::{
		SqliteOperatorStorage, route,
		sql::{CHECKPOINT_REMOVE_SQL, CHECKPOINT_SET_SQL},
	},
	resident::batch::{DropMarker, FlushBatch},
};

impl SqliteOperatorStorage {
	#[instrument(name = "store::operator::persistent::sqlite::flush_batch", level = "debug", skip(self, batch), fields(
		drop_count = batch.drops.len(),
		state_count = batch.state.len(),
		checkpoint_count = batch.checkpoints.len()
	))]
	pub fn flush_batch(&self, batch: &FlushBatch) {
		if !batch.state.is_empty() {
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

		batch.state.write_into(&transaction);

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
