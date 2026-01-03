// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Table storage for memory backend.

use std::collections::BTreeMap;

use crate::tier::Store;

/// Convert TableId to a unique string key for storage in a HashMap
pub(super) fn table_id_to_key(table: Store) -> String {
	match table {
		Store::Multi => "multi".to_string(),
		Store::Single => "single".to_string(),
		Store::Cdc => "cdc".to_string(),
		Store::Source(id) => format!("source:{}", id),
		Store::Operator(id) => format!("operator:{}", id),
	}
}

/// Table storage using string keys for flexibility
pub(super) struct Tables {
	pub(super) data: BTreeMap<String, BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
}

impl Default for Tables {
	fn default() -> Self {
		Self {
			data: BTreeMap::new(),
		}
	}
}

impl Tables {
	pub(super) fn get_table(&self, table: Store) -> Option<&BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
		self.data.get(&table_id_to_key(table))
	}

	pub(super) fn get_table_mut(&mut self, table: Store) -> &mut BTreeMap<Vec<u8>, Option<Vec<u8>>> {
		self.data.entry(table_id_to_key(table)).or_default()
	}
}
