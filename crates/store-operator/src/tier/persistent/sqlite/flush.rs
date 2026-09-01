// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_sqlite::batch::values_placeholders;
use rusqlite::{ToSql, Transaction, TransactionBehavior, params, params_from_iter};
use tracing::instrument;

use crate::tier::{
	persistent::sqlite::{
		SqliteOperatorStorage,
		join_expiry::encode_group,
		route,
		sql::{
			CHECKPOINT_REMOVE_SQL, CHECKPOINT_SET_SQL, JOIN_EXPIRIES_DROP_GROUP_SQL,
			JOIN_EXPIRIES_DROP_OPERATOR_SQL, JOIN_EXPIRY_REMOVE_SQL, JOIN_EXPIRY_SET_SQL,
		},
	},
	resident::batch::{DropMarker, FlushBatch},
};

const FLUSH_CHUNK: usize = 100;

static JOIN_EXPIRY_SET_CHUNK_SQL: LazyLock<String> = LazyLock::new(|| {
	format!(
		r#"INSERT INTO "operator_join_expiry" ("operator", "group", "side", "row_number", "at") VALUES {}
		   ON CONFLICT ("operator", "group", "side", "row_number") DO UPDATE SET "at" = excluded."at""#,
		values_placeholders(FLUSH_CHUNK, 5)
	)
});

static JOIN_EXPIRY_REMOVE_CHUNK_SQL: LazyLock<String> = LazyLock::new(|| {
	format!(
		r#"DELETE FROM "operator_join_expiry" WHERE ("operator", "group", "side", "row_number") IN (VALUES {})"#,
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
	#[instrument(name = "store::operator::persistent::sqlite::flush_batch", level = "debug", skip(self, batch), fields(
		drop_count = batch.drops.len(),
		state_count = batch.state.len(),
		join_expiry_count = batch.join_expiries.len(),
		checkpoint_count = batch.checkpoints.len()
	))]
	pub fn flush_batch(&self, batch: &FlushBatch) {
		if !batch.state.is_empty() {
			self.mark_state_written();
		}
		for ((operator, group, side, row_number), entry) in &batch.join_expiries {
			if entry.is_some() {
				self.join_expiry_filter().add((*operator, *group, *side, *row_number));
			}
		}
		let guard = self.inner.conn.lock();
		let conn = guard.as_ref().expect("operator state flush ran without an open connection");
		let transaction = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
			.expect("operator state flush could not begin");
		for marker in &batch.drops {
			match marker {
				DropMarker::OperatorState(operator) => {
					route::drop_operator(&transaction, *operator);
					transaction
						.prepare_cached(JOIN_EXPIRIES_DROP_OPERATOR_SQL)
						.expect("join expiry operator delete could not be prepared")
						.execute(params![operator.0 as i64])
						.expect("join expiry operator delete failed");
				}
				DropMarker::JoinExpiriesOperator(operator) => {
					transaction
						.prepare_cached(JOIN_EXPIRIES_DROP_OPERATOR_SQL)
						.expect("join expiry operator delete could not be prepared")
						.execute(params![operator.0 as i64])
						.expect("join expiry operator delete failed");
				}
				DropMarker::JoinExpiriesGroup(operator, group) => {
					transaction
						.prepare_cached(JOIN_EXPIRIES_DROP_GROUP_SQL)
						.expect("join expiry group delete could not be prepared")
						.execute(params![operator.0 as i64, encode_group(*group)])
						.expect("join expiry group delete failed");
				}
			}
		}

		batch.state.write_into(&transaction);

		let mut join_expiry_sets: Vec<Vec<Box<dyn ToSql>>> = Vec::new();
		let mut join_expiry_removes: Vec<Vec<Box<dyn ToSql>>> = Vec::new();
		for ((operator, group, side, row_number), entry) in &batch.join_expiries {
			match entry {
				Some(millis) => join_expiry_sets.push(vec![
					Box::new(operator.0 as i64),
					Box::new(encode_group(*group)),
					Box::new(*side as i64),
					Box::new(row_number.0 as i64),
					Box::new(*millis as i64),
				]),
				None => join_expiry_removes.push(vec![
					Box::new(operator.0 as i64),
					Box::new(encode_group(*group)),
					Box::new(*side as i64),
					Box::new(row_number.0 as i64),
				]),
			}
		}
		execute_chunked(&transaction, &JOIN_EXPIRY_SET_CHUNK_SQL, JOIN_EXPIRY_SET_SQL, &join_expiry_sets);
		execute_chunked(
			&transaction,
			&JOIN_EXPIRY_REMOVE_CHUNK_SQL,
			JOIN_EXPIRY_REMOVE_SQL,
			&join_expiry_removes,
		);

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
