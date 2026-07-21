// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_transaction::dictionary::DictionaryAllocatorRegistry;

use crate::transaction::row_allocator::RowAllocatorRegistry;

#[derive(Clone, Default)]
pub struct FlowAllocators {
	pub row: RowAllocatorRegistry,
	pub dictionary: DictionaryAllocatorRegistry,
}

impl FlowAllocators {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_dictionary(dictionary: DictionaryAllocatorRegistry) -> Self {
		Self {
			row: RowAllocatorRegistry::new(),
			dictionary,
		}
	}
}
