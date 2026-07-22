// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_transaction::dictionary::DictionaryAllocatorRegistry;

use crate::transaction::row_number::RowNumberProvider;

#[derive(Clone, Default)]
pub struct FlowAllocators {
	pub row: RowNumberProvider,
	pub dictionary: DictionaryAllocatorRegistry,
}

impl FlowAllocators {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_dictionary(dictionary: DictionaryAllocatorRegistry) -> Self {
		Self {
			row: RowNumberProvider::default(),
			dictionary,
		}
	}
}
