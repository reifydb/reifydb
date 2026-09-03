// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, pod::EncodedPodRow},
};
use reifydb_core::{interface::catalog::flow::OperatorId, key::operator::state::GroupId, metrics::scan::record_page};
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};
use rusqlite::{Connection, Transaction, TransactionBehavior};
use tracing::instrument;

use crate::{
	tier::persistent::sqlite::{SqliteOperatorStorage, route},
	types::OperatorBatch,
};

impl SqliteOperatorStorage {
	#[instrument(name = "store::operator::persistent::sqlite::state_sizes", level = "trace", skip(self, keys), fields(operator = operator.0, key_count = keys.len()))]
	pub fn state_sizes(&self, operator: OperatorId, keys: &[EncodedKey]) -> HashMap<EncodedKey, ByteSize> {
		let mut sizes = HashMap::with_capacity(keys.len());
		if keys.is_empty() || !self.state_written() {
			return sizes;
		}
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return sizes;
		};
		for key in keys {
			if let Some(bytes) = route::get(conn, operator, key) {
				sizes.insert(key.clone(), ByteSize::from_bytes(bytes.len() as u64));
			}
		}
		sizes
	}

	#[instrument(name = "store::operator::persistent::sqlite::get_many", level = "trace", skip(self, keys), fields(operator = operator.0, key_count = keys.len()))]
	pub fn get_many(&self, operator: OperatorId, keys: &[EncodedKey]) -> HashMap<EncodedKey, EncodedPodRow> {
		let mut found = HashMap::with_capacity(keys.len());
		if keys.is_empty() || !self.state_written() {
			return found;
		}
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return found;
		};
		for key in keys {
			if let Some(bytes) = route::get(conn, operator, key) {
				found.insert(key.clone(), decode_row(bytes));
			}
		}
		found
	}

	#[instrument(name = "store::operator::persistent::sqlite::get", level = "trace", skip(self, key), fields(operator = operator.0, key_len = key.len()))]
	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedPodRow> {
		if !self.state_written() {
			return None;
		}
		let guard = self.read_conn();
		let conn = guard.as_ref()?;
		route::get(conn, operator, key).map(decode_row)
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
		route::get(conn, operator, key).is_some()
	}

	#[instrument(name = "store::operator::persistent::sqlite::range_batch", level = "trace", skip(self, range), fields(operator = operator.0, batch_size = batch_size))]
	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		self.page(operator, range, batch_size, false)
	}

	#[instrument(name = "store::operator::persistent::sqlite::last_batch", level = "trace", skip(self, range), fields(operator = operator.0, batch_size = batch_size))]
	pub fn last_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		self.page(operator, range, batch_size, true)
	}

	#[instrument(name = "store::operator::persistent::sqlite::group_page", level = "trace", skip(self, groups), fields(operator = operator.0, group_count = groups.len(), batch_size = batch_size))]
	pub fn group_page(&self, operator: OperatorId, groups: &[GroupId], batch_size: u64) -> OperatorBatch {
		if groups.is_empty() || !self.state_written() {
			return OperatorBatch::empty();
		}
		let limit = batch_size.max(1);
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return OperatorBatch::empty();
		};
		let rows = route::bounded_in(
			conn,
			operator,
			groups,
			&EncodedKeyRange::all(),
			limit.saturating_add(1),
			false,
		);
		record_page(rows.len() as u64, 0);
		let has_more = rows.len() as u64 > limit;
		let mut items: Vec<(EncodedKey, EncodedPodRow)> =
			rows.into_iter().map(|(key, bytes)| (key, decode_row(bytes))).collect();
		items.truncate(limit as usize);
		OperatorBatch {
			items,
			has_more,
			resume: None,
		}
	}

	fn page(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64, reverse: bool) -> OperatorBatch {
		if !self.state_written() {
			return OperatorBatch::empty();
		}
		let limit = batch_size.max(1);
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return OperatorBatch::empty();
		};
		let rows = route::bounded(conn, operator, &range, limit.saturating_add(1), reverse);
		record_page(rows.len() as u64, 0);
		let has_more = rows.len() as u64 > limit;
		let mut items: Vec<(EncodedKey, EncodedPodRow)> =
			rows.into_iter().map(|(key, bytes)| (key, decode_row(bytes))).collect();
		items.truncate(limit as usize);
		OperatorBatch {
			items,
			has_more,
			resume: None,
		}
	}

	#[instrument(name = "store::operator::persistent::sqlite::drop_operator_state", level = "debug", skip(self), fields(operator = operator.0))]
	pub fn drop_operator_state(&self, operator: OperatorId) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		let transaction = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)
			.expect("operator state drop could not begin");
		route::drop_operator(&transaction, operator);
		transaction.commit().expect("operator state drop could not commit");
	}
}

pub(super) fn state_exists(conn: &Connection) -> bool {
	route::census(conn).iter().any(|entry| entry.keys > 0)
}

pub(super) fn decode_row(bytes: Vec<u8>) -> EncodedPodRow {
	EncodedPodRow::from(EncodedBytes(CowVec::new(bytes)))
}
