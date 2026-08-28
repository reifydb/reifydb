// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Debug, Formatter};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_filter::source::{FilterSlice, KeyFilterSource};
use reifydb_store::filter::FilterDomain;

use crate::tier::persistent::{filter::OperatorKeys, sqlite::SqliteOperatorStorage};

pub struct OperatorStateKeySource {
	storage: SqliteOperatorStorage,
	cursor: Option<(OperatorId, EncodedKey)>,
}

impl OperatorStateKeySource {
	pub fn new(storage: SqliteOperatorStorage) -> Self {
		Self {
			storage,
			cursor: None,
		}
	}
}

impl Debug for OperatorStateKeySource {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("OperatorStateKeySource").field("cursor", &self.cursor).finish()
	}
}

impl KeyFilterSource for OperatorStateKeySource {
	fn name(&self) -> &'static str {
		"operator-state"
	}

	fn estimated_len(&self) -> u64 {
		self.storage.state_key_count()
	}

	fn restart(&mut self) {
		self.cursor = None;
	}

	fn next_slice(&mut self, budget: usize) -> FilterSlice {
		let rows = self.storage.state_key_slice(self.cursor.as_ref(), budget);
		let exhausted = rows.len() < budget;
		if let Some((operator, key)) = rows.last() {
			self.cursor = Some((*operator, key.clone()));
		}
		FilterSlice {
			hashes: rows.iter().map(|(operator, key)| OperatorKeys::hash((*operator, key))).collect(),
			exhausted,
		}
	}
}
