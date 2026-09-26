// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::interface::{
	catalog::{
		dictionary::Dictionary,
		id::{TableId, ViewId},
		object::ObjectId,
		table::Table,
		view::View,
	},
	change::Diff,
};
use reifydb_rql::flow::flow::FlowDag;
use reifydb_runtime::context::clock::MockClock;
use reifydb_value::{
	Result,
	value::{
		Value,
		datetime::DateTime,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

use crate::txn::{Changes, ClockNow, Emit, Intern, Lookup, Rows};

pub struct MemoryTxn {
	pub entries: Vec<(ObjectId, Diff)>,
	pub cursor: usize,
	pub rows: BTreeMap<EncodedKey, EncodedBytes>,
	pub emitted: Vec<(ViewId, Diff)>,
	pub flows: Vec<FlowDag>,
	pub views: BTreeMap<ViewId, View>,
	pub tables: BTreeMap<TableId, Table>,
	pub dictionaries: BTreeMap<DictionaryId, Dictionary>,
	pub dictionary_values: BTreeMap<DictionaryId, Vec<Value>>,
	pub clock: MockClock,
}

impl Default for MemoryTxn {
	fn default() -> Self {
		Self {
			entries: Vec::new(),
			cursor: 0,
			rows: BTreeMap::new(),
			emitted: Vec::new(),
			flows: Vec::new(),
			views: BTreeMap::new(),
			tables: BTreeMap::new(),
			dictionaries: BTreeMap::new(),
			dictionary_values: BTreeMap::new(),
			clock: MockClock::from_millis(0),
		}
	}
}

impl Changes for MemoryTxn {
	fn cursor(&self) -> usize {
		self.cursor
	}

	fn entries_from(&self, at: usize) -> Vec<(ObjectId, Diff)> {
		if at >= self.entries.len() {
			return Vec::new();
		}
		self.entries[at..].to_vec()
	}

	fn set_cursor(&mut self, at: usize) {
		assert!(
			at <= self.entries.len(),
			"cursor {} is past the {} entries of the memory transaction",
			at,
			self.entries.len()
		);
		self.cursor = at;
	}
}

impl Rows for MemoryTxn {
	fn get(&mut self, key: &EncodedKey) -> Result<Option<EncodedBytes>> {
		Ok(self.rows.get(key).cloned())
	}

	fn set(&mut self, key: &EncodedKey, row: EncodedBytes) -> Result<()> {
		self.rows.insert(key.clone(), row);
		Ok(())
	}

	fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.rows.remove(key);
		Ok(())
	}
}

impl Emit for MemoryTxn {
	fn emit(&mut self, view: ViewId, diff: Diff) -> Result<()> {
		self.emitted.push((view, diff));
		Ok(())
	}
}

impl Lookup for MemoryTxn {
	fn transactional_flows(&mut self) -> Result<Vec<FlowDag>> {
		Ok(self.flows.clone())
	}

	fn view(&mut self, id: ViewId) -> Result<View> {
		Ok(self.views
			.get(&id)
			.unwrap_or_else(|| panic!("view {} is not in the memory transaction", id))
			.clone())
	}

	fn table(&mut self, id: TableId) -> Result<Table> {
		Ok(self.tables
			.get(&id)
			.unwrap_or_else(|| panic!("table {} is not in the memory transaction", id))
			.clone())
	}

	fn dictionary(&mut self, id: DictionaryId) -> Result<Dictionary> {
		Ok(self.dictionaries
			.get(&id)
			.unwrap_or_else(|| panic!("dictionary {} is not in the memory transaction", id))
			.clone())
	}
}

impl Intern for MemoryTxn {
	fn intern(&mut self, dictionary: &Dictionary, value: &Value) -> Result<DictionaryEntryId> {
		let values = self.dictionary_values.entry(dictionary.id).or_default();
		let position = match values.iter().position(|existing| existing == value) {
			Some(position) => position,
			None => {
				values.push(value.clone());
				values.len() - 1
			}
		};
		DictionaryEntryId::from_u128(position as u128, dictionary.id_type.clone())
	}

	fn resolve(&mut self, dictionary: &Dictionary, id: DictionaryEntryId) -> Result<Option<Value>> {
		let Ok(position) = usize::try_from(id.to_u128()) else {
			return Ok(None);
		};
		Ok(self.dictionary_values.get(&dictionary.id).and_then(|values| values.get(position)).cloned())
	}
}

impl ClockNow for MemoryTxn {
	fn now(&self) -> DateTime {
		self.clock.now()
	}
}
