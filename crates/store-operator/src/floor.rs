// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::{key::encoded::EncodedKey, row::operator::EncodedOperatorRow};
use reifydb_core::key::operator_state::{Keyspace, OperatorStateKey, group_data_of_inner};
use reifydb_value::value::datetime::DateTime;

#[derive(Debug, Clone, Default)]
pub struct FloorSpec {
	data: Option<DateTime>,
	cutoffs: BTreeMap<Keyspace, DateTime>,
}

impl FloorSpec {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn data(cutoff: DateTime) -> Self {
		Self {
			data: Some(cutoff),
			cutoffs: BTreeMap::new(),
		}
	}

	pub fn with(mut self, keyspace: Keyspace, cutoff: DateTime) -> Self {
		self.set(keyspace, cutoff);
		self
	}

	pub fn set(&mut self, keyspace: Keyspace, cutoff: DateTime) {
		self.cutoffs.insert(keyspace, cutoff);
	}

	pub fn set_data(&mut self, cutoff: DateTime) {
		self.data = Some(cutoff);
	}

	pub fn cutoff(&self, keyspace: Keyspace) -> Option<DateTime> {
		match self.cutoffs.get(&keyspace).copied() {
			Some(cutoff) => Some(cutoff),
			None => self.data.filter(|_| keyspace.is_data()),
		}
	}

	pub fn data_cutoff(&self) -> Option<DateTime> {
		self.data
	}

	pub fn max_cutoff(&self) -> Option<DateTime> {
		self.cutoffs.values().copied().chain(self.data).max()
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_none() && self.cutoffs.is_empty()
	}
}

pub(crate) fn floor_expired(floor: &FloorSpec, key: &EncodedKey, row: &EncodedOperatorRow) -> bool {
	if floor.is_empty() {
		return false;
	}
	let Some(group) = group_data_of_inner(key.as_slice()) else {
		return false;
	};
	if group.is_root() {
		return false;
	}
	let Some((_, keyspace, _)) = OperatorStateKey::decode_inner(key.as_slice()) else {
		return false;
	};
	let Some(cutoff) = floor.cutoff(keyspace) else {
		return false;
	};
	row.time() < cutoff
}
