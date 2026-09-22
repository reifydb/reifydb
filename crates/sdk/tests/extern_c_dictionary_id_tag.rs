// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[path = "extern_c/common.rs"]
mod common;

use common::round_trip_column;
use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::{
	container::dictionary_array::{self, dictionary_array},
	dictionary::{DictionaryEntryId, DictionaryId},
};

fn dictionary_parts(buffer: &ColumnBuffer) -> (Vec<DictionaryEntryId>, Option<DictionaryId>) {
	match buffer {
		ColumnBuffer::DictionaryId {
			container,
			dictionary_id,
		} => (dictionary_array::iter(container).collect(), *dictionary_id),
		other => panic!("expected a dictionary id column, got {:?}", other.get_type()),
	}
}

#[test]
fn an_extern_c_round_trip_keeps_the_dictionary_id_of_the_input_column() {
	// Entry ids without their dictionary id can never be decoded, so the host must re-attach the input's id.
	let entries = vec![DictionaryEntryId::U4(1), DictionaryEntryId::U4(7)];
	let input = ColumnBuffer::DictionaryId {
		container: dictionary_array(entries.clone()),
		dictionary_id: Some(DictionaryId(42)),
	};
	let output = round_trip_column("d", input);
	assert_eq!(dictionary_parts(&output), (entries, Some(DictionaryId(42))));
}
