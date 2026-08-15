// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use rusqlite::params;
use tracing::instrument;

use crate::{
	commit::batch::{DropMarker, FlushBatch},
	sqlite::{
		SqliteOperatorStorage,
		sql::{
			ANCHOR_REMOVE_SQL, ANCHOR_SET_SQL, ANCHORS_DROP_GROUP_SQL, ANCHORS_DROP_OPERATOR_SQL,
			CHECKPOINT_REMOVE_SQL, CHECKPOINT_SET_SQL, STATE_DROP_SQL, STATE_REMOVE_SQL, STATE_SET_SQL,
		},
	},
	types::OperatorWrite,
};

impl SqliteOperatorStorage {
	#[instrument(name = "store::operator::persistent::sqlite::apply_batch", level = "debug", skip(self, writes), fields(write_count = writes.len()))]
	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		if writes.is_empty() {
			return;
		}
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		let transaction = conn.unchecked_transaction().expect("operator state batch could not begin");
		for write in writes {
			match write {
				OperatorWrite::Set {
					operator,
					key,
					row,
				} => transaction
					.prepare_cached(STATE_SET_SQL)
					.expect("operator state write could not be prepared")
					.execute(params![operator.0 as i64, key.as_slice(), &row.bytes()[..]])
					.expect("operator state write failed"),
				OperatorWrite::Remove {
					operator,
					key,
				} => transaction
					.prepare_cached(STATE_REMOVE_SQL)
					.expect("operator state delete could not be prepared")
					.execute(params![operator.0 as i64, key.as_slice()])
					.expect("operator state delete failed"),
				OperatorWrite::AnchorSet {
					operator,
					group,
					side,
					row_num: row_number,
					expiry,
				} => transaction
					.prepare_cached(ANCHOR_SET_SQL)
					.expect("seal anchor write could not be prepared")
					.execute(params![
						operator.0 as i64,
						group.0 as i64,
						*side as i64,
						row_number.0 as i64,
						expiry.to_millis() as i64
					])
					.expect("seal anchor write failed"),
				OperatorWrite::AnchorRemove {
					operator,
					group,
					side,
					run_num: row_number,
				} => transaction
					.prepare_cached(ANCHOR_REMOVE_SQL)
					.expect("seal anchor delete could not be prepared")
					.execute(params![
						operator.0 as i64,
						group.0 as i64,
						*side as i64,
						row_number.0 as i64
					])
					.expect("seal anchor delete failed"),
			};
		}
		transaction.commit().expect("operator state batch could not commit");
	}

	#[instrument(name = "store::operator::persistent::sqlite::flush_batch", level = "debug", skip(self, batch), fields(
		drop_count = batch.drops.len(),
		state_count = batch.state.len(),
		anchor_count = batch.anchors.len(),
		checkpoint_count = batch.checkpoints.len()
	))]
	pub fn flush_batch(&self, batch: &FlushBatch) {
		let guard = self.inner.conn.lock();
		let conn = guard.as_ref().expect("operator state flush ran without an open connection");
		let transaction = conn.unchecked_transaction().expect("operator state flush could not begin");

		for marker in &batch.drops {
			match marker {
				DropMarker::OperatorState(operator) => {
					transaction
						.prepare_cached(STATE_DROP_SQL)
						.expect("operator state drop could not be prepared")
						.execute(params![operator.0 as i64])
						.expect("operator state drop failed");
					transaction
						.prepare_cached(ANCHORS_DROP_OPERATOR_SQL)
						.expect("seal anchor operator delete could not be prepared")
						.execute(params![operator.0 as i64])
						.expect("seal anchor operator delete failed");
				}
				DropMarker::AnchorsOperator(operator) => {
					transaction
						.prepare_cached(ANCHORS_DROP_OPERATOR_SQL)
						.expect("seal anchor operator delete could not be prepared")
						.execute(params![operator.0 as i64])
						.expect("seal anchor operator delete failed");
				}
				DropMarker::AnchorsGroup(operator, group) => {
					transaction
						.prepare_cached(ANCHORS_DROP_GROUP_SQL)
						.expect("seal anchor group delete could not be prepared")
						.execute(params![operator.0 as i64, group.0 as i64])
						.expect("seal anchor group delete failed");
				}
			}
		}

		for ((operator, key), entry) in &batch.state {
			match entry {
				Some(row) => transaction
					.prepare_cached(STATE_SET_SQL)
					.expect("operator state write could not be prepared")
					.execute(params![operator.0 as i64, key.as_slice(), &row.bytes()[..]])
					.expect("operator state write failed"),
				None => transaction
					.prepare_cached(STATE_REMOVE_SQL)
					.expect("operator state delete could not be prepared")
					.execute(params![operator.0 as i64, key.as_slice()])
					.expect("operator state delete failed"),
			};
		}

		for ((operator, group, side, row_number), entry) in &batch.anchors {
			match entry {
				Some(millis) => transaction
					.prepare_cached(ANCHOR_SET_SQL)
					.expect("seal anchor write could not be prepared")
					.execute(params![
						operator.0 as i64,
						group.0 as i64,
						*side as i64,
						row_number.0 as i64,
						*millis as i64
					])
					.expect("seal anchor write failed"),
				None => transaction
					.prepare_cached(ANCHOR_REMOVE_SQL)
					.expect("seal anchor delete could not be prepared")
					.execute(params![
						operator.0 as i64,
						group.0 as i64,
						*side as i64,
						row_number.0 as i64
					])
					.expect("seal anchor delete failed"),
			};
		}

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
