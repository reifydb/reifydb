// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_transaction::dictionary::DictionaryAllocatorRegistry;

use crate::transaction::{
	group::GroupInterner, row_number::RowNumberProvider, timer::TimerWheel, watermark::SourceWatermarks,
};

#[derive(Clone, Default)]
pub struct FlowSubstrate {
	pub row: RowNumberProvider,
	pub group: GroupInterner,
	pub dictionary: DictionaryAllocatorRegistry,
	pub watermarks: SourceWatermarks,
	pub timers: TimerWheel,
}

impl FlowSubstrate {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn with_dictionary(dictionary: DictionaryAllocatorRegistry) -> Self {
		Self {
			dictionary,
			..Self::default()
		}
	}
}
