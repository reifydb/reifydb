// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_sqlite::batch::values_placeholders;
use rusqlite::{ToSql, Transaction, params, params_from_iter};
use tracing::instrument;

use crate::{
	sqlite::{
		SqliteOperatorStorage,
		sql::{
			ANCHOR_REMOVE_SQL, ANCHOR_SET_SQL, ANCHORS_DROP_GROUP_SQL, ANCHORS_DROP_OPERATOR_SQL,
			CHECKPOINT_REMOVE_SQL, CHECKPOINT_SET_SQL, STATE_DROP_SQL, STATE_REMOVE_SQL, STATE_SET_SQL,
		},
	},
	tier::commit::batch::{DropMarker, FlushBatch},
	types::OperatorWrite,
};

const FLUSH_CHUNK: usize = 100;

static STATE_SET_CHUNK_SQL: LazyLock<String> = LazyLock::new(|| {
	format!(
		r#"INSERT INTO "operator_state" ("operator", "key", "bytes") VALUES {}
		   ON CONFLICT ("operator", "key") DO UPDATE SET "bytes" = excluded."bytes""#,
		values_placeholders(FLUSH_CHUNK, 3)
	)
});

static STATE_REMOVE_CHUNK_SQL: LazyLock<String> = LazyLock::new(|| {
	format!(
		r#"DELETE FROM "operator_state" WHERE ("operator", "key") IN (VALUES {})"#,
		values_placeholders(FLUSH_CHUNK, 2)
	)
});

static ANCHOR_SET_CHUNK_SQL: LazyLock<String> = LazyLock::new(|| {
	format!(
		r#"INSERT INTO "operator_seal_anchor" ("operator", "group", "side", "row_number", "expiry") VALUES {}
		   ON CONFLICT ("operator", "group", "side", "row_number") DO UPDATE SET "expiry" = excluded."expiry""#,
		values_placeholders(FLUSH_CHUNK, 5)
	)
});

static ANCHOR_REMOVE_CHUNK_SQL: LazyLock<String> = LazyLock::new(|| {
	format!(
		r#"DELETE FROM "operator_seal_anchor" WHERE ("operator", "group", "side", "row_number") IN (VALUES {})"#,
		values_placeholders(FLUSH_CHUNK, 4)
	)
});

fn execute_chunked(txn: &Transaction, chunk_sql: &str, single_sql: &str, rows: &[Vec<Box<dyn ToSql>>]) {
	if rows.is_empty() {
		return;
	}
	let mut chunk_stmt =
		txn.prepare_cached(chunk_sql).expect("chunked operator state statement could not be prepared");
	let mut single_stmt = txn.prepare_cached(single_sql).expect("operator state statement could not be prepared");
	let mut chunks = rows.chunks_exact(FLUSH_CHUNK);
	for full in chunks.by_ref() {
		let flat: Vec<&dyn ToSql> = full.iter().flat_map(|row| row.iter().map(|p| p.as_ref())).collect();
		chunk_stmt.execute(params_from_iter(flat)).expect("chunked operator state write failed");
	}
	for row in chunks.remainder() {
		single_stmt
			.execute(params_from_iter(row.iter().map(|p| p.as_ref())))
			.expect("operator state write failed");
	}
}

impl SqliteOperatorStorage {
	#[instrument(name = "store::operator::persistent::sqlite::apply_batch", level = "debug", skip(self, writes), fields(write_count = writes.len()))]
	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		if writes.is_empty() {
			return;
		}
		self.mark_state_written();
		for write in writes {
			if let OperatorWrite::Set {
				operator,
				key,
				..
			} = write
			{
				self.filter().add(*operator, key);
			}
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
					row: post,
				}
				| OperatorWrite::Insert {
					operator,
					key,
					post,
				}
				| OperatorWrite::Replace {
					operator,
					key,
					post,
					..
				} => transaction
					.prepare_cached(STATE_SET_SQL)
					.expect("operator state write could not be prepared")
					.execute(params![operator.0 as i64, key.as_slice(), &post.bytes()[..]])
					.expect("operator state write failed"),
				OperatorWrite::Remove {
					operator,
					key,
					..
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
				}
				| OperatorWrite::AnchorInsert {
					operator,
					group,
					side,
					row_num: row_number,
					expiry,
				}
				| OperatorWrite::AnchorReplace {
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
					row_num: row_number,
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
		if !batch.state.is_empty() {
			self.mark_state_written();
		}
		for ((operator, key), entry) in &batch.state {
			if entry.post.is_some() {
				self.filter().add(*operator, key);
			}
		}
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

		let mut state_sets: Vec<Vec<Box<dyn ToSql>>> = Vec::new();
		let mut state_removes: Vec<Vec<Box<dyn ToSql>>> = Vec::new();
		for ((operator, key), entry) in &batch.state {
			match &entry.post {
				Some(row) => state_sets.push(vec![
					Box::new(operator.0 as i64),
					Box::new(key.as_slice().to_vec()),
					Box::new(row.bytes()[..].to_vec()),
				]),
				None => state_removes
					.push(vec![Box::new(operator.0 as i64), Box::new(key.as_slice().to_vec())]),
			}
		}
		execute_chunked(&transaction, &STATE_SET_CHUNK_SQL, STATE_SET_SQL, &state_sets);
		execute_chunked(&transaction, &STATE_REMOVE_CHUNK_SQL, STATE_REMOVE_SQL, &state_removes);

		let mut anchor_sets: Vec<Vec<Box<dyn ToSql>>> = Vec::new();
		let mut anchor_removes: Vec<Vec<Box<dyn ToSql>>> = Vec::new();
		for ((operator, group, side, row_number), entry) in &batch.anchors {
			match entry {
				Some(millis) => anchor_sets.push(vec![
					Box::new(operator.0 as i64),
					Box::new(group.0 as i64),
					Box::new(*side as i64),
					Box::new(row_number.0 as i64),
					Box::new(*millis as i64),
				]),
				None => anchor_removes.push(vec![
					Box::new(operator.0 as i64),
					Box::new(group.0 as i64),
					Box::new(*side as i64),
					Box::new(row_number.0 as i64),
				]),
			}
		}
		execute_chunked(&transaction, &ANCHOR_SET_CHUNK_SQL, ANCHOR_SET_SQL, &anchor_sets);
		execute_chunked(&transaction, &ANCHOR_REMOVE_CHUNK_SQL, ANCHOR_REMOVE_SQL, &anchor_removes);

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
