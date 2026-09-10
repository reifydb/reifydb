// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, pod::EncodedPodRow},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId},
	metrics::scan::record_page,
};
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};
use rusqlite::{Connection, Transaction, TransactionBehavior};
use tracing::instrument;

use crate::{
	persistent::sqlite::{SqlitePersistent, route},
	types::OperatorBatch,
};

impl SqlitePersistent {
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
		for (key, bytes) in route::get_many(conn, operator, keys) {
			sizes.insert(key, ByteSize::from_bytes(bytes.len() as u64));
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
		for (key, bytes) in route::get_many(conn, operator, keys) {
			found.insert(key, decode_row(bytes));
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
	pub fn group_page(
		&self,
		operator: OperatorId,
		groups: &[GroupId],
		batch_size: u64,
		mask: u64,
	) -> OperatorBatch {
		if groups.is_empty() || !self.state_written() {
			return OperatorBatch::empty();
		}
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return OperatorBatch::empty();
		};
		let rows = route::bounded_in(
			conn,
			operator,
			groups,
			&EncodedKeyRange::all(),
			batch_size.saturating_add(1),
			false,
			mask,
		);
		record_page(rows.len() as u64, 0);
		let items: Vec<(GroupStateKey, EncodedPodRow)> = rows
			.into_iter()
			.map(|(key, bytes)| (GroupStateKey::bound_unchecked(key), decode_row(bytes)))
			.collect();
		into_batch(items, batch_size)
	}

	fn page(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64, reverse: bool) -> OperatorBatch {
		if !self.state_written() {
			return OperatorBatch::empty();
		}
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return OperatorBatch::empty();
		};
		let rows = route::bounded(conn, operator, &range, batch_size.saturating_add(1), reverse);
		record_page(rows.len() as u64, 0);
		let items: Vec<(GroupStateKey, EncodedPodRow)> = rows
			.into_iter()
			.map(|(key, bytes)| (GroupStateKey::bound_unchecked(key), decode_row(bytes)))
			.collect();
		into_batch(items, batch_size)
	}

	pub fn state_keys_after(
		&self,
		operator: OperatorId,
		keyspace: KeyspaceId,
		after: Option<&EncodedKey>,
		limit: u64,
	) -> Vec<EncodedKey> {
		if !self.state_written() {
			return Vec::new();
		}
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		route::keys_after(conn, operator, keyspace, after, limit)
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

fn limit_of(batch: u64) -> usize {
	usize::try_from(batch).unwrap_or(usize::MAX)
}

fn into_batch(mut items: Vec<(GroupStateKey, EncodedPodRow)>, batch: u64) -> OperatorBatch {
	let limit = limit_of(batch);
	match items.len() > limit {
		true => {
			let resume = items[limit].0.clone();
			items.truncate(limit);
			OperatorBatch {
				items,
				has_more: true,
				resume: Some(resume),
			}
		}
		false => OperatorBatch {
			items,
			has_more: false,
			resume: None,
		},
	}
}

pub(super) fn state_exists(conn: &Connection) -> bool {
	route::census(conn).iter().any(|entry| entry.keys > 0)
}

pub(super) fn decode_row(bytes: Vec<u8>) -> EncodedPodRow {
	EncodedPodRow::from(EncodedBytes(CowVec::new(bytes)))
}
