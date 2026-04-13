// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{
	container::dictionary::DictionaryContainer, dictionary::DictionaryEntryId, frame::data::FrameColumnData,
	r#type::Type,
};

fn make(v: Vec<DictionaryEntryId>) -> FrameColumnData {
	FrameColumnData::DictionaryId(DictionaryContainer::new(v))
}

crate::nones_tests! {
	values: vec![
		DictionaryEntryId::U16(1),
		DictionaryEntryId::U16(1000),
		DictionaryEntryId::U16(1_000_000),
		DictionaryEntryId::U16(0),
		DictionaryEntryId::U16(u16::MAX as u128),
	],
	inner_type: Type::DictionaryId,
}
