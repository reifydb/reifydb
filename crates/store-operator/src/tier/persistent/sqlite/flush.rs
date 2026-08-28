// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_assertions)]
use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

#[cfg(reifydb_assertions)]
use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
#[cfg(reifydb_assertions)]
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_sqlite::batch::values_placeholders;
#[cfg(reifydb_assertions)]
use reifydb_value::byte_size::ByteSize;
use reifydb_value::reifydb_assertions;
#[cfg(reifydb_assertions)]
use rusqlite::OptionalExtension;
use rusqlite::{ToSql, Transaction, TransactionBehavior, params, params_from_iter};
use tracing::instrument;

#[cfg(reifydb_assertions)]
use crate::{tier::persistent::sqlite::sql::STATE_VALUE_LEN_SQL, types::DurablePre};
use crate::{
	tier::{
		persistent::sqlite::{
			SqliteOperatorStorage,
			census::{batch_delta, flush_delta, zero_operator_buckets},
			join_expiry::encode_group,
			sql::{
				CHECKPOINT_REMOVE_SQL, CHECKPOINT_SET_SQL, JOIN_EXPIRIES_DROP_GROUP_SQL,
				JOIN_EXPIRIES_DROP_OPERATOR_SQL, JOIN_EXPIRY_REMOVE_SQL, JOIN_EXPIRY_SET_SQL,
				STATE_DROP_SQL, STATE_REMOVE_SQL, STATE_SET_SQL,
			},
		},
		resident::batch::{DropMarker, FlushBatch},
	},
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
	#[instrument(name = "store::operator::persistent::sqlite::apply_batch", level = "debug", skip(self, writes), fields(write_count = writes.len()))]
	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		if writes.is_empty() {
			return;
		}
		self.mark_state_written();
		for write in writes {
			match write {
				OperatorWrite::Insert {
					operator,
					key,
					..
				}
				| OperatorWrite::Replace {
					operator,
					key,
					..
				} => self.filter().add((*operator, key)),
				OperatorWrite::JoinExpiryInsert {
					operator,
					group,
					side,
					row_num,
					..
				}
				| OperatorWrite::JoinExpiryReplace {
					operator,
					group,
					side,
					row_num,
					..
				} => self.join_expiry_filter().add((*operator, *group, *side, *row_num)),
				_ => {}
			}
		}
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		let transaction = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
			.expect("operator state batch could not begin");
		reifydb_assertions! {
			verify_batch_classification(&transaction, writes);
		}
		for write in writes {
			match write {
				OperatorWrite::Insert {
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
				OperatorWrite::JoinExpiryInsert {
					operator,
					group,
					side,
					row_num: row_number,
					at,
				}
				| OperatorWrite::JoinExpiryReplace {
					operator,
					group,
					side,
					row_num: row_number,
					at,
				} => transaction
					.prepare_cached(JOIN_EXPIRY_SET_SQL)
					.expect("join expiry write could not be prepared")
					.execute(params![
						operator.0 as i64,
						encode_group(*group),
						*side as i64,
						row_number.0 as i64,
						at.to_millis() as i64
					])
					.expect("join expiry write failed"),
				OperatorWrite::JoinExpiryRemove {
					operator,
					group,
					side,
					row_num: row_number,
					..
				} => transaction
					.prepare_cached(JOIN_EXPIRY_REMOVE_SQL)
					.expect("join expiry delete could not be prepared")
					.execute(params![
						operator.0 as i64,
						encode_group(*group),
						*side as i64,
						row_number.0 as i64
					])
					.expect("join expiry delete failed"),
			};
		}
		batch_delta(writes).apply(&transaction);
		transaction.commit().expect("operator state batch could not commit");
	}

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
		for ((operator, key), entry) in &batch.state {
			if entry.post.is_some() {
				self.filter().add((operator, key));
			}
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
		reifydb_assertions! {
			verify_flush_classification(&transaction, batch);
		}

		for marker in &batch.drops {
			match marker {
				DropMarker::OperatorState(operator) => {
					transaction
						.prepare_cached(STATE_DROP_SQL)
						.expect("operator state drop could not be prepared")
						.execute(params![operator.0 as i64])
						.expect("operator state drop failed");
					transaction
						.prepare_cached(JOIN_EXPIRIES_DROP_OPERATOR_SQL)
						.expect("join expiry operator delete could not be prepared")
						.execute(params![operator.0 as i64])
						.expect("join expiry operator delete failed");
					zero_operator_buckets(&transaction, *operator);
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
		flush_delta(batch).apply(&transaction);

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

#[cfg(reifydb_assertions)]
fn value_bytes(row: &EncodedPodRow) -> ByteSize {
	ByteSize::from_bytes(row.bytes().len() as u64)
}

#[cfg(reifydb_assertions)]
fn durable_value_len(transaction: &Transaction, operator: OperatorId, key: &EncodedKey) -> Option<ByteSize> {
	transaction
		.prepare_cached(STATE_VALUE_LEN_SQL)
		.expect("operator state pre-image probe could not be prepared")
		.query_row(params![operator.0 as i64, key.as_slice()], |row| row.get::<_, i64>(0))
		.optional()
		.expect("operator state pre-image probe failed")
		.map(|len| ByteSize::from_bytes(len as u64))
}

#[cfg(reifydb_assertions)]
fn assert_claim(operator: OperatorId, claimed: DurablePre, observed: Option<ByteSize>) {
	let claimed = match claimed {
		DurablePre::Absent => None,
		DurablePre::Present(bytes) => Some(bytes),
	};
	assert_eq!(
		claimed, observed,
		"operator {} classified a durable write against a pre-image sqlite does not hold; the census is \
		 delta arithmetic over that claim, so a wrong one drifts the bucket until the next reseed",
		operator.0
	);
}

#[cfg(reifydb_assertions)]
fn verify_batch_classification(transaction: &Transaction, writes: &[OperatorWrite]) {
	let mut overlay: BTreeMap<(OperatorId, EncodedKey), Option<ByteSize>> = BTreeMap::new();
	for write in writes {
		let (operator, key, claimed, post) = match write {
			OperatorWrite::Insert {
				operator,
				key,
				post,
			} => (*operator, key, DurablePre::Absent, Some(value_bytes(post))),
			OperatorWrite::Replace {
				operator,
				key,
				pre_value_bytes,
				post,
			} => (*operator, key, DurablePre::Present(*pre_value_bytes), Some(value_bytes(post))),
			OperatorWrite::Remove {
				operator,
				key,
				pre,
			} => (*operator, key, *pre, None),
			_ => continue,
		};
		let slot = (operator, key.clone());
		let observed = match overlay.get(&slot) {
			Some(pending) => *pending,
			None => durable_value_len(transaction, operator, key),
		};
		assert_claim(operator, claimed, observed);
		overlay.insert(slot, post);
	}
}

#[cfg(reifydb_assertions)]
fn verify_flush_classification(transaction: &Transaction, batch: &FlushBatch) {
	let dropped: BTreeSet<OperatorId> = batch
		.drops
		.iter()
		.filter_map(|marker| match marker {
			DropMarker::OperatorState(operator) => Some(*operator),
			_ => None,
		})
		.collect();
	for ((operator, key), entry) in &batch.state {
		let observed = match dropped.contains(&operator) {
			true => None,
			false => durable_value_len(transaction, operator, key),
		};
		assert_claim(operator, entry.durable_pre, observed);
	}
}
