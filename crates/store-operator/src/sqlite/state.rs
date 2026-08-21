// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, pod::EncodedPodRow},
};
use reifydb_core::{interface::catalog::flow::OperatorId, metrics::scan::record_page};
use reifydb_value::util::cowvec::CowVec;
use rusqlite::{Connection, Rows, ToSql, params};
use tracing::instrument;

use crate::{
	sqlite::{
		SqliteOperatorStorage,
		sql::{
			ANCHORS_DROP_OPERATOR_SQL, STATE_CONTAINS_SQL, STATE_DROP_SQL, STATE_EXISTS_SQL, STATE_GET_SQL,
			STATE_KEY_COUNT_SQL, STATE_KEYS_AFTER_SQL, STATE_KEYS_FIRST_SQL, STATE_REMOVE_SQL,
			STATE_SET_SQL, range_sql,
		},
	},
	types::OperatorBatch,
};

impl SqliteOperatorStorage {
	#[instrument(name = "store::operator::persistent::sqlite::set", level = "debug", skip(self, key, row), fields(operator = operator.0, key_len = key.len()))]
	pub fn set(&self, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
		self.mark_state_written();
		self.filter().add(operator, &key);
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.execute(STATE_SET_SQL, params![operator.0 as i64, key.as_slice(), &row.bytes()[..]])
			.expect("operator state write failed");
	}

	#[instrument(name = "store::operator::persistent::sqlite::remove", level = "debug", skip(self, key), fields(operator = operator.0, key_len = key.len()))]
	pub fn remove(&self, operator: OperatorId, key: &EncodedKey) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.execute(STATE_REMOVE_SQL, params![operator.0 as i64, key.as_slice()])
			.expect("operator state delete failed");
	}

	#[instrument(name = "store::operator::persistent::sqlite::get", level = "trace", skip(self, key), fields(operator = operator.0, key_len = key.len()))]
	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedPodRow> {
		if !self.state_written() {
			return None;
		}
		let guard = self.read_conn();
		let conn = guard.as_ref()?;
		let mut stmt = conn.prepare_cached(STATE_GET_SQL).expect("operator state read could not be prepared");
		let mut rows =
			stmt.query(params![operator.0 as i64, key.as_slice()]).expect("operator state read failed");
		let row = rows.next().expect("operator state read failed")?;
		let bytes: Vec<u8> = row.get(0).expect("operator state rows carry a blob payload");
		Some(decode_row(bytes))
	}

	#[instrument(name = "store::operator::persistent::sqlite::contains", level = "trace", skip(self, key), fields(operator = operator.0, key_len = key.len()), ret)]
	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		if !self.state_written() {
			return false;
		}
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return false;
		};
		let mut stmt =
			conn.prepare_cached(STATE_CONTAINS_SQL).expect("operator state probe could not be prepared");
		let mut rows =
			stmt.query(params![operator.0 as i64, key.as_slice()]).expect("operator state probe failed");
		rows.next().expect("operator state probe failed").is_some()
	}

	#[instrument(name = "store::operator::persistent::sqlite::range_batch", level = "trace", skip(self, range), fields(operator = operator.0, batch_size = batch_size))]
	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		if !self.state_written() {
			return OperatorBatch::empty();
		}
		let sql = range_sql(range.start.as_ref(), range.end.as_ref());
		let mut blobs: Vec<&[u8]> = Vec::with_capacity(2);
		if let Bound::Included(key) | Bound::Excluded(key) = range.start.as_ref() {
			blobs.push(key.as_slice());
		}
		if let Bound::Included(key) | Bound::Excluded(key) = range.end.as_ref() {
			blobs.push(key.as_slice());
		}

		let limit = batch_size.max(1);
		let operator_param = operator.0 as i64;
		let limit_param = limit.min(i64::MAX as u64 - 1) as i64 + 1;
		let mut bound_params: Vec<&dyn ToSql> = Vec::with_capacity(blobs.len() + 2);
		bound_params.push(&operator_param);
		for blob in &blobs {
			bound_params.push(blob);
		}
		bound_params.push(&limit_param);

		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return OperatorBatch::empty();
		};
		let mut stmt = conn.prepare_cached(sql).expect("operator state scan could not be prepared");
		let mut rows = stmt.query(bound_params.as_slice()).expect("operator state scan failed");

		let mut items = Vec::new();
		while let Some(row) = rows.next().expect("operator state scan failed") {
			let key: Vec<u8> = row.get(0).expect("operator state rows carry a blob key");
			let bytes: Vec<u8> = row.get(1).expect("operator state rows carry a blob payload");
			items.push((EncodedKey::new(key), decode_row(bytes)));
		}
		record_page(items.len() as u64, 0);

		let has_more = items.len() as u64 > limit;
		items.truncate(limit as usize);
		OperatorBatch {
			items,
			has_more,
		}
	}

	#[instrument(name = "store::operator::persistent::sqlite::drop_operator_state", level = "debug", skip(self), fields(operator = operator.0))]
	pub fn drop_operator_state(&self, operator: OperatorId) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		let transaction = conn.unchecked_transaction().expect("operator state drop could not begin");
		transaction.execute(STATE_DROP_SQL, params![operator.0 as i64]).expect("operator state drop failed");
		transaction
			.execute(ANCHORS_DROP_OPERATOR_SQL, params![operator.0 as i64])
			.expect("seal anchor drop failed");
		transaction.commit().expect("operator state drop could not commit");
	}

	pub(crate) fn state_key_count(&self) -> u64 {
		let guard = self.read_conn();
		let conn = guard.as_ref().expect("operator state key count ran without an open connection");
		let count: i64 = conn
			.query_row(STATE_KEY_COUNT_SQL, [], |row| row.get(0))
			.expect("operator state key count failed");
		count.max(0) as u64
	}

	pub(crate) fn state_key_slice(
		&self,
		cursor: Option<&(OperatorId, EncodedKey)>,
		budget: usize,
	) -> Vec<(OperatorId, EncodedKey)> {
		let limit = budget as i64;
		let guard = self.read_conn();
		let conn = guard.as_ref().expect("operator state key scan ran without an open connection");
		let mut rows = Vec::new();
		match cursor {
			Some((operator, key)) => {
				let mut stmt = conn
					.prepare_cached(STATE_KEYS_AFTER_SQL)
					.expect("operator state key scan could not be prepared");
				let mut cursor_rows = stmt
					.query(params![operator.0 as i64, key.as_slice(), limit])
					.expect("operator state key scan failed");
				collect_keys(&mut cursor_rows, &mut rows);
			}
			None => {
				let mut stmt = conn
					.prepare_cached(STATE_KEYS_FIRST_SQL)
					.expect("operator state key scan could not be prepared");
				let mut first_rows =
					stmt.query(params![limit]).expect("operator state key scan failed");
				collect_keys(&mut first_rows, &mut rows);
			}
		}
		rows
	}
}

pub(super) fn state_exists(conn: &Connection) -> bool {
	let exists: i64 =
		conn.query_row(STATE_EXISTS_SQL, [], |row| row.get(0)).expect("operator state existence probe failed");
	exists != 0
}

fn collect_keys(rows: &mut Rows<'_>, out: &mut Vec<(OperatorId, EncodedKey)>) {
	while let Some(row) = rows.next().expect("operator state key scan failed") {
		let operator: i64 = row.get(0).expect("operator state rows carry an operator id");
		let key: Vec<u8> = row.get(1).expect("operator state rows carry a blob key");
		out.push((OperatorId(operator as u64), EncodedKey::new(key)));
	}
}

pub(super) fn decode_row(bytes: Vec<u8>) -> EncodedPodRow {
	EncodedPodRow::from(EncodedBytes(CowVec::new(bytes)))
}
