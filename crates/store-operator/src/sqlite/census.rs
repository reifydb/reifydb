// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::OperatorStateKey};
use reifydb_value::byte_size::ByteSize;
use rusqlite::{Transaction, params};
use tracing::instrument;

use crate::{
	sqlite::{
		SqliteOperatorStorage,
		sql::{
			ANCHOR_CENSUS_SQL, ANCHOR_COUNT_SQL, ANCHOR_TOTAL_COUNT_SQL, CENSUS_APPLY_SQL,
			CENSUS_ZERO_OPERATOR_SQL, STATE_BYTES_SQL, STATE_CENSUS_SQL, STATE_TOTAL_BYTES_SQL,
		},
	},
	tier::commit::batch::FlushBatch,
	types::{
		ANCHOR_KEY_BYTES, ANCHOR_VALUE_BYTES, DurablePre, OperatorSealAnchorCensus, OperatorStateCensus,
		OperatorWrite,
	},
};

impl SqliteOperatorStorage {
	#[instrument(name = "store::operator::persistent::sqlite::bytes", level = "trace", skip(self), fields(operator = operator.0), ret)]
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

	#[instrument(name = "store::operator::persistent::sqlite::total_bytes", level = "trace", skip(self), ret)]
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

	#[instrument(name = "store::operator::persistent::sqlite::census", level = "debug", skip(self))]
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

	#[instrument(name = "store::operator::persistent::sqlite::anchor_census", level = "debug", skip(self))]
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

#[derive(Default)]
struct StateDelta {
	keys: i64,
	key_bytes: i64,
	value_bytes: i64,
}

impl StateDelta {
	fn is_zero(&self) -> bool {
		self.keys == 0 && self.key_bytes == 0 && self.value_bytes == 0
	}
}

#[derive(Default)]
pub(super) struct CensusDelta {
	buckets: BTreeMap<(OperatorId, u8), StateDelta>,
}

impl CensusDelta {
	fn record(&mut self, operator: OperatorId, key: &EncodedKey, pre: DurablePre, post: Option<u64>) {
		let bucket = self.buckets.entry((operator, keyspace_byte(operator, key))).or_default();
		match (pre, post) {
			(DurablePre::Absent, Some(post)) => {
				bucket.keys += 1;
				bucket.key_bytes += key.len() as i64;
				bucket.value_bytes += post as i64;
			}
			(DurablePre::Present(pre), Some(post)) => {
				bucket.value_bytes += post as i64 - pre.as_bytes() as i64;
			}
			(DurablePre::Present(pre), None) => {
				bucket.keys -= 1;
				bucket.key_bytes -= key.len() as i64;
				bucket.value_bytes -= pre.as_bytes() as i64;
			}
			(DurablePre::Absent, None) => {}
		}
	}

	pub(super) fn apply(&self, transaction: &Transaction) {
		for ((operator, keyspace), delta) in &self.buckets {
			if delta.is_zero() {
				continue;
			}
			let stored = [*keyspace];
			transaction
				.prepare_cached(CENSUS_APPLY_SQL)
				.expect("operator state census update could not be prepared")
				.execute(params![
					operator.0 as i64,
					stored.as_slice(),
					delta.keys,
					delta.key_bytes,
					delta.value_bytes
				])
				.expect("operator state census update failed");
		}
	}
}

pub(super) fn batch_delta(writes: &[OperatorWrite]) -> CensusDelta {
	let mut delta = CensusDelta::default();
	for write in writes {
		match write {
			OperatorWrite::Insert {
				operator,
				key,
				post,
			} => delta.record(*operator, key, DurablePre::Absent, Some(post.bytes().len() as u64)),
			OperatorWrite::Replace {
				operator,
				key,
				pre_value_bytes,
				post,
			} => delta.record(
				*operator,
				key,
				DurablePre::Present(*pre_value_bytes),
				Some(post.bytes().len() as u64),
			),
			OperatorWrite::Remove {
				operator,
				key,
				pre,
			} => delta.record(*operator, key, *pre, None),
			OperatorWrite::AnchorInsert {
				..
			}
			| OperatorWrite::AnchorReplace {
				..
			}
			| OperatorWrite::AnchorRemove {
				..
			} => {}
		}
	}
	delta
}

pub(super) fn flush_delta(batch: &FlushBatch) -> CensusDelta {
	let mut delta = CensusDelta::default();
	for ((operator, key), entry) in &batch.state {
		delta.record(
			*operator,
			key,
			entry.durable_pre,
			entry.post.as_ref().map(|row| row.bytes().len() as u64),
		);
	}
	delta
}

pub(super) fn zero_operator_buckets(transaction: &Transaction, operator: OperatorId) {
	transaction
		.prepare_cached(CENSUS_ZERO_OPERATOR_SQL)
		.expect("operator state census reset could not be prepared")
		.execute(params![operator.0 as i64])
		.expect("operator state census reset failed");
}

fn keyspace_byte(operator: OperatorId, key: &EncodedKey) -> u8 {
	*key.as_slice().get(OperatorStateKey::KEYSPACE_INNER_OFFSET as usize).unwrap_or_else(|| {
		panic!(
			"operator {} wrote a state key with no keyspace byte; every census bucket is keyed on it",
			operator.0
		)
	})
}
