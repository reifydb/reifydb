// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Table storage for memory backend.

use std::collections::BTreeMap;

use crate::tier::TableId;

/// Convert TableId to a unique string key for storage in a HashMap
pub(super) fn table_id_to_key(table: TableId) -> String {
	match table {
		TableId::Multi => "multi".to_string(),
		TableId::Single => "single".to_string(),
		TableId::Cdc => "cdc".to_string(),
		TableId::Source(id) => format!("source:{}", id),
		TableId::Operator(id) => format!("operator:{}", id),
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
	pub(super) fn get_table(&self, table: TableId) -> Option<&BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
		self.data.get(&table_id_to_key(table))
	}

	pub(super) fn get_table_mut(&mut self, table: TableId) -> &mut BTreeMap<Vec<u8>, Option<Vec<u8>>> {
		self.data.entry(table_id_to_key(table)).or_default()
	}
}
