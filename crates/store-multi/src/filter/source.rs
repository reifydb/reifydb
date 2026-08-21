// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Debug, Formatter};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::interface::store::EntryKind;
use reifydb_filter::source::{FilterSlice, KeyFilterSource};

use crate::{filter::hash_key, tier::persistent::sqlite::storage::SqlitePersistentStorage};

pub struct MultiCurrentKeySource {
	storage: SqlitePersistentStorage,
	tables: Vec<EntryKind>,
	listed: bool,
	index: usize,
	cursor: Option<EncodedKey>,
}

impl MultiCurrentKeySource {
	pub fn new(storage: SqlitePersistentStorage) -> Self {
		Self {
			storage,
			tables: Vec::new(),
			listed: false,
			index: 0,
			cursor: None,
		}
	}

	fn ensure_listed(&mut self) {
		if self.listed {
			return;
		}
		self.tables = self.storage.list_current_entries().expect("multi current table listing failed");
		self.listed = true;
	}
}

impl Debug for MultiCurrentKeySource {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("MultiCurrentKeySource")
			.field("tables", &self.tables.len())
			.field("index", &self.index)
			.field("cursor", &self.cursor)
			.finish()
	}
}

impl KeyFilterSource for MultiCurrentKeySource {
	fn name(&self) -> &'static str {
		"multi-current"
	}

	fn estimated_len(&self) -> u64 {
		let tables = self.storage.list_current_entries().expect("multi current table listing failed");
		let mut total = 0u64;
		for table in tables {
			total = total
				.saturating_add(self.storage.count_current(table).expect("multi current count failed"));
		}
		total
	}

	fn restart(&mut self) {
		self.tables = Vec::new();
		self.listed = false;
		self.index = 0;
		self.cursor = None;
	}

	fn next_slice(&mut self, budget: usize) -> FilterSlice {
		self.ensure_listed();
		let mut hashes = Vec::with_capacity(budget);
		while hashes.len() < budget {
			let Some(&table) = self.tables.get(self.index) else {
				break;
			};
			let want = budget - hashes.len();
			let keys = self
				.storage
				.current_key_slice(table, self.cursor.as_ref(), want)
				.expect("multi current key scan failed");
			let fetched = keys.len();
			if let Some(last) = keys.last() {
				self.cursor = Some(last.clone());
			}
			hashes.extend(keys.iter().map(|key| hash_key(table, key)));
			if fetched < want {
				self.index += 1;
				self.cursor = None;
			}
		}
		FilterSlice {
			exhausted: self.index >= self.tables.len(),
			hashes,
		}
	}
}
