// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{
	container::dictionary_array::dictionary_array, dictionary::DictionaryEntryId, value_type::ValueType,
};

use crate::common::{ColumnData, data};

fn make(v: Vec<DictionaryEntryId>) -> ColumnData {
	data(ValueType::DictionaryId, dictionary_array(v))
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
