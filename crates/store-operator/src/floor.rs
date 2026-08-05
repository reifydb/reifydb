// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::{
	encoded::row::{EncodedRow, SHAPE_HEADER_SIZE},
	key::encoded::EncodedKey,
};
use reifydb_core::key::operator_group_state::{Keyspace, OperatorGroupStateKey, group_data_of_inner};
use reifydb_value::value::datetime::DateTime;

#[derive(Debug, Clone, Default)]
pub struct FloorSpec {
	cutoffs: BTreeMap<Keyspace, DateTime>,
}

impl FloorSpec {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with(mut self, keyspace: Keyspace, cutoff: DateTime) -> Self {
		self.set(keyspace, cutoff);
		self
	}

	pub fn set(&mut self, keyspace: Keyspace, cutoff: DateTime) {
		self.cutoffs.insert(keyspace, cutoff);
	}

	pub fn cutoff(&self, keyspace: Keyspace) -> Option<DateTime> {
		self.cutoffs.get(&keyspace).copied()
	}

	pub fn is_empty(&self) -> bool {
		self.cutoffs.is_empty()
	}
}

pub(crate) fn floor_expired(floor: &FloorSpec, key: &EncodedKey, row: &EncodedRow) -> bool {
	if floor.is_empty() {
		return false;
	}
	let Some(group) = group_data_of_inner(key.as_slice()) else {
		return false;
	};
	if group.is_node_scope() {
		return false;
	}
	let Some((_, keyspace, _)) = OperatorGroupStateKey::decode_inner(key.as_slice()) else {
		return false;
	};
	let Some(cutoff) = floor.cutoff(keyspace) else {
		return false;
	};
	if row.as_slice().len() < SHAPE_HEADER_SIZE {
		return false;
	}
	row.updated_at() < cutoff
}
