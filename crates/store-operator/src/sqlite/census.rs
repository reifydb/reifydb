// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::OperatorStateKey};
use reifydb_value::byte_size::ByteSize;
use rusqlite::params;
use tracing::instrument;

use crate::{
	sqlite::{
		SqliteOperatorStorage,
		sql::{
			ANCHOR_CENSUS_SQL, ANCHOR_COUNT_SQL, ANCHOR_TOTAL_COUNT_SQL, STATE_BYTES_SQL, STATE_CENSUS_SQL,
			STATE_TOTAL_BYTES_SQL,
		},
	},
	types::{ANCHOR_KEY_BYTES, ANCHOR_VALUE_BYTES, OperatorSealAnchorCensus, OperatorStateCensus},
};

impl SqliteOperatorStorage {
	#[instrument(name = "store::operator::bytes", level = "trace", skip(self), fields(operator = operator.0), ret)]
	pub fn bytes(&self, operator: OperatorId) -> ByteSize {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return ByteSize::ZERO;
		};
		let state = conn
			.query_row(STATE_BYTES_SQL, params![operator.0 as i64], |row| row.get::<_, i64>(0))
			.expect("operator state size query failed") as u64;
		let anchors = conn
			.query_row(ANCHOR_COUNT_SQL, params![operator.0 as i64], |row| row.get::<_, i64>(0))
			.expect("seal anchor size query failed") as u64;
		ByteSize::from_bytes(state) + (ANCHOR_KEY_BYTES + ANCHOR_VALUE_BYTES) * anchors
	}

	#[instrument(name = "store::operator::total_bytes", level = "trace", skip(self), ret)]
	pub fn total_bytes(&self) -> ByteSize {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return ByteSize::ZERO;
		};
		let state = conn
			.query_row(STATE_TOTAL_BYTES_SQL, [], |row| row.get::<_, i64>(0))
			.expect("operator state size query failed") as u64;
		let anchors = conn
			.query_row(ANCHOR_TOTAL_COUNT_SQL, [], |row| row.get::<_, i64>(0))
			.expect("seal anchor size query failed") as u64;
		ByteSize::from_bytes(state) + (ANCHOR_KEY_BYTES + ANCHOR_VALUE_BYTES) * anchors
	}

	#[instrument(name = "store::operator::census", level = "debug", skip(self))]
	pub fn census(&self) -> Vec<OperatorStateCensus> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		let mut stmt =
			conn.prepare_cached(STATE_CENSUS_SQL).expect("operator state census could not be prepared");
		let mut rows = stmt.query([]).expect("operator state census failed");

		let mut out = Vec::new();
		while let Some(row) = rows.next().expect("operator state census failed") {
			let stored: Vec<u8> = row.get(1).expect("census rows carry a keyspace byte");
			out.push(OperatorStateCensus {
				operator: OperatorId(
					row.get::<_, i64>(0).expect("census rows carry an operator") as u64
				),
				keyspace: OperatorStateKey::decode_keyspace(
					*stored.first().expect("state keys carry a keyspace byte"),
				),
				keys: row.get::<_, i64>(2).expect("census rows carry a key count") as u64,
				key_bytes: ByteSize::from_bytes(
					row.get::<_, i64>(3).expect("census rows carry a key byte sum") as u64,
				),
				value_bytes: ByteSize::from_bytes(
					row.get::<_, i64>(4).expect("census rows carry a value byte sum") as u64,
				),
			});
		}
		out
	}

	#[instrument(name = "store::operator::anchor_census", level = "debug", skip(self))]
	pub fn anchor_census(&self) -> Vec<OperatorSealAnchorCensus> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		let mut stmt =
			conn.prepare_cached(ANCHOR_CENSUS_SQL).expect("seal anchor census could not be prepared");
		let mut rows = stmt.query([]).expect("seal anchor census failed");

		let mut out = Vec::new();
		while let Some(row) = rows.next().expect("seal anchor census failed") {
			out.push(OperatorSealAnchorCensus {
				operator: OperatorId(
					row.get::<_, i64>(0).expect("census rows carry an operator") as u64
				),
				keys: row.get::<_, i64>(1).expect("census rows carry a key count") as u64,
			});
		}
		out
	}
}
