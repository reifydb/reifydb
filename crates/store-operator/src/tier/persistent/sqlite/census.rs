// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_value::byte_size::ByteSize;
use tracing::instrument;

use crate::{
	tier::persistent::sqlite::{SqliteOperatorStorage, route},
	types::OperatorStateCensus,
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
		ByteSize::from_bytes(state)
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
		ByteSize::from_bytes(state)
	}

	#[instrument(name = "store::operator::persistent::sqlite::census", level = "debug", skip(self))]
	pub fn census(&self) -> Vec<OperatorStateCensus> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		route::census(conn)
	}
}
