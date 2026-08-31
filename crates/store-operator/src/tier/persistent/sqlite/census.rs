// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_value::byte_size::ByteSize;
use rusqlite::params;
use tracing::instrument;

use crate::{
	tier::persistent::sqlite::{
		SqliteOperatorStorage,
		route,
		sql::{JOIN_EXPIRY_COUNT_SQL, JOIN_EXPIRY_CENSUS_SQL, JOIN_EXPIRY_TOTAL_COUNT_SQL},
	},
	types::{JOIN_EXPIRY_KEY_BYTES, JOIN_EXPIRY_VALUE_BYTES, OperatorStateCensus, StoredJoinRowExpiryCensus},
};

impl SqliteOperatorStorage {
	#[instrument(name = "store::operator::persistent::sqlite::bytes", level = "trace", skip(self), fields(operator = operator.0), ret)]
	pub fn bytes(&self, operator: OperatorId) -> ByteSize {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return ByteSize::ZERO;
		};
		let state: u64 = route::census(conn)
			.iter()
			.filter(|entry| entry.operator == operator)
			.map(|entry| entry.key_bytes.as_bytes() + entry.value_bytes.as_bytes())
			.sum();
		let join_expiries = conn
			.query_row(JOIN_EXPIRY_COUNT_SQL, params![operator.0 as i64], |row| row.get::<_, i64>(0))
			.expect("join expiry size query failed") as u64;
		ByteSize::from_bytes(state) + (JOIN_EXPIRY_KEY_BYTES + JOIN_EXPIRY_VALUE_BYTES) * join_expiries
	}

	#[instrument(name = "store::operator::persistent::sqlite::total_bytes", level = "trace", skip(self), ret)]
	pub fn total_bytes(&self) -> ByteSize {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return ByteSize::ZERO;
		};
		let state: u64 = route::census(conn)
			.iter()
			.map(|entry| entry.key_bytes.as_bytes() + entry.value_bytes.as_bytes())
			.sum();
		let join_expiries = conn
			.query_row(JOIN_EXPIRY_TOTAL_COUNT_SQL, [], |row| row.get::<_, i64>(0))
			.expect("join expiry size query failed") as u64;
		ByteSize::from_bytes(state) + (JOIN_EXPIRY_KEY_BYTES + JOIN_EXPIRY_VALUE_BYTES) * join_expiries
	}

	#[instrument(name = "store::operator::persistent::sqlite::census", level = "debug", skip(self))]
	pub fn census(&self) -> Vec<OperatorStateCensus> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		route::census(conn)
	}

	#[instrument(name = "store::operator::persistent::sqlite::join_expiry_census", level = "debug", skip(self))]
	pub fn join_expiry_census(&self) -> Vec<StoredJoinRowExpiryCensus> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		let mut stmt =
			conn.prepare_cached(JOIN_EXPIRY_CENSUS_SQL).expect("join expiry census could not be prepared");
		let mut rows = stmt.query([]).expect("join expiry census failed");

		let mut out = Vec::new();
		while let Some(row) = rows.next().expect("join expiry census failed") {
			out.push(StoredJoinRowExpiryCensus {
				operator: OperatorId(
					row.get::<_, i64>(0).expect("census rows carry an operator") as u64
				),
				keys: row.get::<_, i64>(1).expect("census rows carry a key count") as u64,
			});
		}
		out
	}
}
