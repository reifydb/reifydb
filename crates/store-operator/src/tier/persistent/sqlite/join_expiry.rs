// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encode_u128;
use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::GroupId};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};
use rusqlite::{Connection, Rows, params};
use tracing::instrument;

use crate::{
	tier::persistent::sqlite::{
		SqliteOperatorStorage,
		sql::{
			JOIN_EXPIRIES_BY_TIME_SQL, JOIN_EXPIRIES_DROP_GROUP_SQL, JOIN_EXPIRIES_DROP_OPERATOR_SQL,
			JOIN_EXPIRIES_DUE_SQL, JOIN_EXPIRY_EXISTS_SQL, JOIN_EXPIRY_GET_SQL, JOIN_EXPIRY_REMOVE_SQL,
			JOIN_EXPIRY_SET_SQL,
		},
	},
	types::StoredJoinRowExpiry,
};

impl SqliteOperatorStorage {
	#[instrument(name = "store::operator::persistent::sqlite::join_expiry_get", level = "trace", skip(self), fields(operator = operator.0, group = group.0, side = side))]
	pub fn join_expiry_get(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Option<DateTime> {
		let guard = self.read_conn();
		let conn = guard.as_ref()?;
		let mut stmt =
			conn.prepare_cached(JOIN_EXPIRY_GET_SQL).expect("join expiry read could not be prepared");
		let mut rows = stmt
			.query(params![operator.0 as i64, encode_group(group), side as i64, row_number.0 as i64])
			.expect("join expiry read failed");
		let row = rows.next().expect("join expiry read failed")?;
		Some(decode_expiry(row.get(0).expect("join expiries carry an at")))
	}

	#[instrument(name = "store::operator::persistent::sqlite::join_expiries_by_time", level = "trace", skip(self), fields(operator = operator.0, group = group.0, limit = limit))]
	pub fn join_expiries_by_time(
		&self,
		operator: OperatorId,
		group: GroupId,
		limit: u64,
	) -> Vec<StoredJoinRowExpiry> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		let mut stmt =
			conn.prepare_cached(JOIN_EXPIRIES_BY_TIME_SQL).expect("join expiry scan could not be prepared");
		let rows = stmt
			.query(params![operator.0 as i64, encode_group(group), limit as i64])
			.expect("join expiry scan failed");
		collect_join_expiries(rows)
	}

	#[instrument(name = "store::operator::persistent::sqlite::join_expiries_due", level = "trace", skip(self, at), fields(operator = operator.0, group = group.0, limit = limit))]
	pub fn join_expiries_due(
		&self,
		operator: OperatorId,
		group: GroupId,
		at: DateTime,
		limit: u64,
	) -> Vec<StoredJoinRowExpiry> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		let mut stmt =
			conn.prepare_cached(JOIN_EXPIRIES_DUE_SQL).expect("join expiry due scan could not be prepared");
		let rows = stmt
			.query(params![operator.0 as i64, encode_group(group), at.to_millis() as i64, limit as i64])
			.expect("join expiry due scan failed");
		collect_join_expiries(rows)
	}

	#[instrument(name = "store::operator::persistent::sqlite::join_expiry_set", level = "debug", skip(self, expiry), fields(operator = operator.0, group = group.0, side = side))]
	pub fn join_expiry_set(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
		expiry: DateTime,
	) {
		self.mark_join_expiries_out_of_band();
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.prepare_cached(JOIN_EXPIRY_SET_SQL)
			.expect("join expiry write could not be prepared")
			.execute(params![
				operator.0 as i64,
				encode_group(group),
				side as i64,
				row_number.0 as i64,
				expiry.to_millis() as i64
			])
			.expect("join expiry write failed");
	}

	#[instrument(name = "store::operator::persistent::sqlite::join_expiry_remove", level = "debug", skip(self), fields(operator = operator.0, group = group.0, side = side))]
	pub fn join_expiry_remove(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.prepare_cached(JOIN_EXPIRY_REMOVE_SQL)
			.expect("join expiry delete could not be prepared")
			.execute(params![operator.0 as i64, encode_group(group), side as i64, row_number.0 as i64])
			.expect("join expiry delete failed");
	}

	#[instrument(name = "store::operator::persistent::sqlite::join_expiries_remove_group", level = "debug", skip(self), fields(operator = operator.0, group = group.0))]
	pub fn join_expiries_remove_group(&self, operator: OperatorId, group: GroupId) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.prepare_cached(JOIN_EXPIRIES_DROP_GROUP_SQL)
			.expect("join expiry group delete could not be prepared")
			.execute(params![operator.0 as i64, encode_group(group)])
			.expect("join expiry group delete failed");
	}

	#[instrument(name = "store::operator::persistent::sqlite::join_expiries_drop_operator", level = "debug", skip(self), fields(operator = operator.0))]
	pub fn join_expiries_drop_operator(&self, operator: OperatorId) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.prepare_cached(JOIN_EXPIRIES_DROP_OPERATOR_SQL)
			.expect("join expiry operator delete could not be prepared")
			.execute(params![operator.0 as i64])
			.expect("join expiry operator delete failed");
	}
}

pub(super) fn encode_group(group: GroupId) -> [u8; 16] {
	encode_u128(group.0)
}

pub(super) fn join_expiry_exists(conn: &Connection) -> bool {
	let exists: i64 = conn
		.query_row(JOIN_EXPIRY_EXISTS_SQL, [], |row| row.get(0))
		.expect("join expiry existence probe failed");
	exists != 0
}

pub(super) fn decode_expiry(millis: i64) -> DateTime {
	DateTime::from_millis(u64::try_from(millis).expect("join expiries are written as unsigned millis"))
}

fn collect_join_expiries(mut rows: Rows<'_>) -> Vec<StoredJoinRowExpiry> {
	let mut out = Vec::new();
	while let Some(row) = rows.next().expect("join expiry scan failed") {
		out.push(StoredJoinRowExpiry {
			side: u8::try_from(row.get::<_, i64>(0).expect("join expiries carry a side"))
				.expect("join expiry sides are written as bytes"),
			row_number: RowNumber(row.get::<_, i64>(1).expect("join expiries carry a row number") as u64),
			at: decode_expiry(row.get(2).expect("join expiries carry an at")),
		});
	}
	out
}
