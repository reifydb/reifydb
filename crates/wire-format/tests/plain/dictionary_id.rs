// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{
	container::dictionary::DictionaryContainer, dictionary::DictionaryEntryId, frame::data::FrameColumnData,
};

fn make(v: Vec<DictionaryEntryId>) -> FrameColumnData {
	FrameColumnData::DictionaryId(DictionaryContainer::new(v))
}

crate::plain_tests! {
	typical: vec![
		DictionaryEntryId::U16(1),
		DictionaryEntryId::U16(1000),
		DictionaryEntryId::U16(1_000_000),
		DictionaryEntryId::U16(1_000_000_000_000),
	],
	boundary: vec![
		DictionaryEntryId::U16(0),
		DictionaryEntryId::U16(u8::MAX as u128),
		DictionaryEntryId::U16(u16::MAX as u128),
		DictionaryEntryId::U16(u32::MAX as u128),
		DictionaryEntryId::U16(u64::MAX as u128),
		DictionaryEntryId::U16(u128::MAX),
	],
	single: DictionaryEntryId::U16(1),
}
