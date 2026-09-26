// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			keyspace::{KeyspaceVisitor, columns_width},
			state::{KEYSPACE_INNER_PREFIX_LEN, KeyspaceId},
			traits::Keyspace,
		},
		typed::layout::KeyLayout,
	},
};
use reifydb_value::byte_size::ByteSize;
use rusqlite::Connection;
use tracing::instrument;

use crate::{
	persistent::sqlite::{SqlitePersistent, route, typed},
	types::OperatorStateCensus,
};

pub(super) struct Census<'a> {
	pub(super) conn: &'a Connection,
	pub(super) operator: OperatorId,
}

impl KeyspaceVisitor for Census<'_> {
	type Output = Option<OperatorStateCensus>;

	fn visit<K: Keyspace>(self) -> Self::Output {
		let width = (KEYSPACE_INNER_PREFIX_LEN + columns_width(<K::Suffix as KeyLayout>::COLUMNS)) as u64;
		let (keys, value_bytes) = typed::census::<K>(self.conn, self.operator);
		(keys > 0).then(|| OperatorStateCensus {
			operator: self.operator,
			keyspace: K::ID,
			keys,
			key_bytes: ByteSize::from_bytes(keys * width),
			value_bytes: ByteSize::from_bytes(value_bytes),
		})
	}
}

impl SqlitePersistent {
	#[instrument(name = "store::operator::persistent::sqlite::bytes", level = "trace", skip(self), fields(operator = operator.0), ret)]
	pub fn bytes(&self, operator: OperatorId) -> ByteSize {
		let tables = self.inner.tables.snapshot();
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return ByteSize::ZERO;
		};
		let state: u64 = route::census(conn, &tables)
			.iter()
			.filter(|entry| entry.operator == operator)
			.map(|entry| entry.key_bytes.as_bytes() + entry.value_bytes.as_bytes())
			.sum();
		ByteSize::from_bytes(state)
	}

	#[instrument(name = "store::operator::persistent::sqlite::occupied_keyspaces", level = "debug", skip(self), fields(operator = operator.0))]
	pub fn occupied_keyspaces(&self, operator: OperatorId) -> Vec<KeyspaceId> {
		let tables = self.inner.tables.mask(operator);
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		route::occupied_keyspaces(conn, operator, tables)
	}

	#[instrument(name = "store::operator::persistent::sqlite::census", level = "debug", skip(self))]
	pub fn census(&self) -> Vec<OperatorStateCensus> {
		let tables = self.inner.tables.snapshot();
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		route::census(conn, &tables)
	}
}
