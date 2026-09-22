// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::dictionary_array::dictionary_array, dictionary::DictionaryEntryId, frame::data::FrameColumnData,
	value_type::ValueType,
};

fn make(v: Vec<DictionaryEntryId>) -> FrameColumnData {
	FrameColumnData::DictionaryId {
		container: dictionary_array(v),
		dictionary_id: None,
	}
}

crate::nones_tests! {
	values: vec![
		DictionaryEntryId::U16(1),
		DictionaryEntryId::U16(1000),
		DictionaryEntryId::U16(1_000_000),
		DictionaryEntryId::U16(0),
		DictionaryEntryId::U16(u16::MAX as u128),
	],
	inner_type: ValueType::DictionaryId,
}
