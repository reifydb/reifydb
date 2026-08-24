// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::GroupId};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};
use rusqlite::{Connection, Rows, params};
use tracing::instrument;

use crate::{
	sqlite::{
		SqliteOperatorStorage,
		sql::{
			ANCHOR_EXISTS_SQL, ANCHOR_GET_SQL, ANCHOR_REMOVE_SQL, ANCHOR_SET_SQL,
			ANCHORS_BY_EXPIRY_SQL, ANCHORS_DROP_GROUP_SQL, ANCHORS_DROP_OPERATOR_SQL, ANCHORS_DUE_SQL,
		},
	},
	types::OperatorSealAnchor,
};

impl SqliteOperatorStorage {
	#[instrument(name = "store::operator::persistent::sqlite::anchor_get", level = "trace", skip(self), fields(operator = operator.0, group = group.0, side = side))]
	pub fn anchor_get(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Option<DateTime> {
		let guard = self.read_conn();
		let conn = guard.as_ref()?;
		let mut stmt = conn.prepare_cached(ANCHOR_GET_SQL).expect("seal anchor read could not be prepared");
		let mut rows = stmt
			.query(params![operator.0 as i64, group.0 as i64, side as i64, row_number.0 as i64])
			.expect("seal anchor read failed");
		let row = rows.next().expect("seal anchor read failed")?;
		Some(decode_expiry(row.get(0).expect("seal anchors carry an expiry")))
	}

	#[instrument(name = "store::operator::persistent::sqlite::anchors_by_expiry", level = "trace", skip(self), fields(operator = operator.0, group = group.0, limit = limit))]
	pub fn anchors_by_expiry(&self, operator: OperatorId, group: GroupId, limit: u64) -> Vec<OperatorSealAnchor> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		let mut stmt =
			conn.prepare_cached(ANCHORS_BY_EXPIRY_SQL).expect("seal anchor scan could not be prepared");
		let rows = stmt
			.query(params![operator.0 as i64, group.0 as i64, limit as i64])
			.expect("seal anchor scan failed");
		collect_anchors(rows)
	}

	#[instrument(name = "store::operator::persistent::sqlite::anchors_due", level = "trace", skip(self, at), fields(operator = operator.0, group = group.0, limit = limit))]
	pub fn anchors_due(
		&self,
		operator: OperatorId,
		group: GroupId,
		at: DateTime,
		limit: u64,
	) -> Vec<OperatorSealAnchor> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		let mut stmt =
			conn.prepare_cached(ANCHORS_DUE_SQL).expect("seal anchor due scan could not be prepared");
		let rows = stmt
			.query(params![operator.0 as i64, group.0 as i64, at.to_millis() as i64, limit as i64])
			.expect("seal anchor due scan failed");
		collect_anchors(rows)
	}

	#[instrument(name = "store::operator::persistent::sqlite::anchor_set", level = "debug", skip(self, expiry), fields(operator = operator.0, group = group.0, side = side))]
	pub fn anchor_set(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
		expiry: DateTime,
	) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.prepare_cached(ANCHOR_SET_SQL)
			.expect("seal anchor write could not be prepared")
			.execute(params![
				operator.0 as i64,
				group.0 as i64,
				side as i64,
				row_number.0 as i64,
				expiry.to_millis() as i64
			])
			.expect("seal anchor write failed");
	}

	#[instrument(name = "store::operator::persistent::sqlite::anchor_remove", level = "debug", skip(self), fields(operator = operator.0, group = group.0, side = side))]
	pub fn anchor_remove(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.prepare_cached(ANCHOR_REMOVE_SQL)
			.expect("seal anchor delete could not be prepared")
			.execute(params![operator.0 as i64, group.0 as i64, side as i64, row_number.0 as i64])
			.expect("seal anchor delete failed");
	}

	#[instrument(name = "store::operator::persistent::sqlite::anchors_remove_group", level = "debug", skip(self), fields(operator = operator.0, group = group.0))]
	pub fn anchors_remove_group(&self, operator: OperatorId, group: GroupId) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.prepare_cached(ANCHORS_DROP_GROUP_SQL)
			.expect("seal anchor group delete could not be prepared")
			.execute(params![operator.0 as i64, group.0 as i64])
			.expect("seal anchor group delete failed");
	}

	#[instrument(name = "store::operator::persistent::sqlite::anchors_drop_operator", level = "debug", skip(self), fields(operator = operator.0))]
	pub fn anchors_drop_operator(&self, operator: OperatorId) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.prepare_cached(ANCHORS_DROP_OPERATOR_SQL)
			.expect("seal anchor operator delete could not be prepared")
			.execute(params![operator.0 as i64])
			.expect("seal anchor operator delete failed");
	}
}

pub(super) fn anchor_exists(conn: &Connection) -> bool {
	let exists: i64 =
		conn.query_row(ANCHOR_EXISTS_SQL, [], |row| row.get(0)).expect("seal anchor existence probe failed");
	exists != 0
}

pub(super) fn decode_expiry(millis: i64) -> DateTime {
	DateTime::from_millis(u64::try_from(millis).expect("seal anchor expiries are written as unsigned millis"))
}

fn collect_anchors(mut rows: Rows<'_>) -> Vec<OperatorSealAnchor> {
	let mut out = Vec::new();
	while let Some(row) = rows.next().expect("seal anchor scan failed") {
		out.push(OperatorSealAnchor {
			side: u8::try_from(row.get::<_, i64>(0).expect("seal anchors carry a side"))
				.expect("seal anchor sides are written as bytes"),
			row_number: RowNumber(row.get::<_, i64>(1).expect("seal anchors carry a row number") as u64),
			expiry: decode_expiry(row.get(2).expect("seal anchors carry an expiry")),
		});
	}
	out
}
