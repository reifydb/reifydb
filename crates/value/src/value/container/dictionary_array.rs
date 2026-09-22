// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::result::Result as StdResult;

use arrow_array::{Array, FixedSizeBinaryArray};
use arrow_buffer::{BooleanBuffer, MutableBuffer};
use serde::{Deserialize, Deserializer, Serializer};

use crate::value::{Value, dictionary::DictionaryEntryId, value_type::ValueType};

pub const DICTIONARY_ENTRY_WIDTH: usize = 17;

pub fn encode(entry: DictionaryEntryId) -> [u8; DICTIONARY_ENTRY_WIDTH] {
	let (tag, value) = match entry {
		DictionaryEntryId::U1(v) => (0u8, v as u128),
		DictionaryEntryId::U2(v) => (1u8, v as u128),
		DictionaryEntryId::U4(v) => (2u8, v as u128),
		DictionaryEntryId::U8(v) => (3u8, v as u128),
		DictionaryEntryId::U16(v) => (4u8, v),
	};
	let mut row = [0u8; DICTIONARY_ENTRY_WIDTH];
	row[0] = tag;
	row[1..].copy_from_slice(&value.to_le_bytes());
	row
}

pub fn decode(row: &[u8; DICTIONARY_ENTRY_WIDTH]) -> DictionaryEntryId {
	let [tag, value @ ..] = *row;
	let value = u128::from_le_bytes(value);
	match tag {
		0 => DictionaryEntryId::U1(value as u8),
		1 => DictionaryEntryId::U2(value as u16),
		2 => DictionaryEntryId::U4(value as u32),
		3 => DictionaryEntryId::U8(value as u64),
		4 => DictionaryEntryId::U16(value),
		other => panic!("dictionary entry row has width byte {other}, expected 0 to 4"),
	}
}

fn assert_whole_rows(bytes: usize) {
	assert_eq!(
		bytes % DICTIONARY_ENTRY_WIDTH,
		0,
		"dictionary id buffer of {bytes} bytes is not a whole number of {DICTIONARY_ENTRY_WIDTH} byte rows"
	);
}

fn rows(array: &FixedSizeBinaryArray) -> &[[u8; DICTIONARY_ENTRY_WIDTH]] {
	assert_eq!(
		array.value_length() as usize,
		DICTIONARY_ENTRY_WIDTH,
		"dictionary id column must hold {DICTIONARY_ENTRY_WIDTH} byte values, found {}",
		array.value_length()
	);
	assert_whole_rows(array.value_data().len());
	array.value_data()[..array.len() * DICTIONARY_ENTRY_WIDTH].as_chunks::<DICTIONARY_ENTRY_WIDTH>().0
}

pub fn push_entry(buffer: &mut MutableBuffer, entry: DictionaryEntryId) {
	buffer.extend_from_slice(&encode(entry));
}

pub fn from_buffer(buffer: MutableBuffer) -> FixedSizeBinaryArray {
	assert_whole_rows(buffer.len());
	FixedSizeBinaryArray::new(DICTIONARY_ENTRY_WIDTH as i32, buffer.into(), None)
}

pub fn dictionary_array(values: impl IntoIterator<Item = DictionaryEntryId>) -> FixedSizeBinaryArray {
	let values = values.into_iter();
	let mut buffer = MutableBuffer::with_capacity(values.size_hint().0 * DICTIONARY_ENTRY_WIDTH);
	for entry in values {
		push_entry(&mut buffer, entry);
	}
	from_buffer(buffer)
}

pub fn iter(array: &FixedSizeBinaryArray) -> impl ExactSizeIterator<Item = DictionaryEntryId> + '_ {
	rows(array).iter().map(decode)
}

pub fn get(array: &FixedSizeBinaryArray, index: usize) -> Option<DictionaryEntryId> {
	rows(array).get(index).map(decode)
}

pub fn get_value(array: &FixedSizeBinaryArray, index: usize) -> Value {
	get(array, index).map(Value::DictionaryId).unwrap_or(Value::none_of(ValueType::DictionaryId))
}

pub fn as_string(array: &FixedSizeBinaryArray, index: usize) -> String {
	get(array, index).map(|id| id.to_string()).unwrap_or_else(|| "none".to_string())
}

pub fn slice(array: &FixedSizeBinaryArray, start: usize, end: usize) -> FixedSizeBinaryArray {
	let end = end.min(array.len());
	let start = start.min(end);
	array.slice(start, end - start)
}

pub fn take(array: &FixedSizeBinaryArray, num: usize) -> FixedSizeBinaryArray {
	slice(array, 0, num)
}

pub fn filter(array: &FixedSizeBinaryArray, mask: &BooleanBuffer) -> FixedSizeBinaryArray {
	let rows = rows(array);
	let mut kept = MutableBuffer::with_capacity(mask.count_set_bits() * DICTIONARY_ENTRY_WIDTH);
	for (i, keep) in mask.iter().enumerate() {
		if keep && let Some(row) = rows.get(i) {
			kept.extend_from_slice(row);
		}
	}
	from_buffer(kept)
}

pub fn reorder(array: &FixedSizeBinaryArray, indices: &[usize]) -> FixedSizeBinaryArray {
	let rows = rows(array);
	let mut reordered = MutableBuffer::with_capacity(indices.len() * DICTIONARY_ENTRY_WIDTH);
	for &idx in indices {
		match rows.get(idx) {
			Some(row) => reordered.extend_from_slice(row),
			None => reordered.extend_zeros(DICTIONARY_ENTRY_WIDTH),
		}
	}
	from_buffer(reordered)
}

pub fn capacity(array: &FixedSizeBinaryArray) -> usize {
	let buffer = array.values();
	if buffer.strong_count() == 1 {
		buffer.capacity() / DICTIONARY_ENTRY_WIDTH
	} else {
		array.len()
	}
}

pub fn heap_size(array: &FixedSizeBinaryArray) -> usize {
	capacity(array) * DICTIONARY_ENTRY_WIDTH
}

pub fn serialize<Ser: Serializer>(array: &FixedSizeBinaryArray, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error> {
	serializer.collect_seq(iter(array))
}

pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<FixedSizeBinaryArray, D::Error> {
	Ok(dictionary_array(Vec::<DictionaryEntryId>::deserialize(deserializer)?))
}

#[cfg(test)]
mod tests {
	use serde::{Deserialize, Serialize};

	use super::*;

	#[derive(Serialize, Deserialize)]
	struct DictionaryColumn(
		#[serde(serialize_with = "serialize", deserialize_with = "deserialize")] FixedSizeBinaryArray,
	);

	fn boundaries() -> Vec<DictionaryEntryId> {
		vec![
			DictionaryEntryId::U1(0),
			DictionaryEntryId::U1(u8::MAX),
			DictionaryEntryId::U2(0),
			DictionaryEntryId::U2(u16::MAX),
			DictionaryEntryId::U4(0),
			DictionaryEntryId::U4(u32::MAX),
			DictionaryEntryId::U8(0),
			DictionaryEntryId::U8(u64::MAX),
			DictionaryEntryId::U16(0),
			DictionaryEntryId::U16(u128::MAX),
		]
	}

	#[test]
	fn every_width_round_trips_at_zero_and_its_max() {
		// A lost width tag turns U2(0) into U1(0), which compares unequal and hashes differently.
		let entries = boundaries();
		for entry in &entries {
			assert_eq!(decode(&encode(*entry)), *entry);
		}
		let array = dictionary_array(entries.clone());
		assert_eq!(array.value_length() as usize, DICTIONARY_ENTRY_WIDTH);
		assert_eq!(array.len(), entries.len());
		assert_eq!(iter(&array).collect::<Vec<_>>(), entries);
		for (i, entry) in entries.iter().enumerate() {
			assert_eq!(get(&array, i), Some(*entry));
			assert_eq!(get_value(&array, i), Value::DictionaryId(*entry));
		}
		assert_eq!(get(&array, entries.len()), None);
		assert_eq!(get_value(&array, entries.len()), Value::none_of(ValueType::DictionaryId));
	}

	#[test]
	fn row_layout_is_width_byte_then_little_endian_u128() {
		// The row bytes are the storage contract; a reordered layout breaks every stored buffer.
		let row = encode(DictionaryEntryId::U4(0x0102_0304));
		assert_eq!(row, [2, 4, 3, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
		let row = encode(DictionaryEntryId::U16(u128::MAX));
		assert_eq!(row[0], 4);
		assert!(row[1..].iter().all(|&b| b == 0xff));
	}

	#[test]
	fn all_zero_row_reads_the_default_entry() {
		// Default and out-of-range rows are zero filled, so the zero row must read U1(0).
		assert_eq!(decode(&[0; DICTIONARY_ENTRY_WIDTH]), DictionaryEntryId::U1(0));
		assert_eq!(decode(&[0; DICTIONARY_ENTRY_WIDTH]), DictionaryEntryId::default());
		let mut buffer = MutableBuffer::new(0);
		buffer.extend_zeros(DICTIONARY_ENTRY_WIDTH * 2);
		let array = from_buffer(buffer);
		assert_eq!(iter(&array).collect::<Vec<_>>(), [DictionaryEntryId::U1(0); 2]);
	}

	#[test]
	#[should_panic(expected = "width byte 5")]
	fn bad_width_byte_panics_naming_the_byte() {
		// Reading an unknown width as some default would silently corrupt dictionary lookups.
		let mut row = [0; DICTIONARY_ENTRY_WIDTH];
		row[0] = 5;
		decode(&row);
	}

	#[test]
	#[should_panic(expected = "not a whole number of 17 byte rows")]
	fn partial_row_buffer_is_refused() {
		// Arrow alone would drop the trailing partial row and hide the corruption.
		let mut buffer = MutableBuffer::new(0);
		push_entry(&mut buffer, DictionaryEntryId::U2(7));
		buffer.push(0u8);
		from_buffer(buffer);
	}

	#[test]
	#[should_panic(expected = "must hold 17 byte values")]
	fn wrong_width_array_is_refused() {
		// A 16 byte uuid array read as dictionary rows would shift every entry.
		let array = FixedSizeBinaryArray::new(16, vec![0u8; 32].into(), None);
		get(&array, 0);
	}

	#[test]
	fn filter_reorder_slice_take_keep_the_rows() {
		// Each op must move whole 17 byte rows; a byte offset slip decodes garbage widths.
		let entries = boundaries();
		let array = dictionary_array(entries.clone());
		let mask = BooleanBuffer::from((0..entries.len()).map(|i| i % 2 == 1).collect::<Vec<_>>());
		let filtered = filter(&array, &mask);
		assert_eq!(
			iter(&filtered).collect::<Vec<_>>(),
			entries.iter().skip(1).step_by(2).copied().collect::<Vec<_>>()
		);
		let reordered = reorder(&array, &[9, 0, 42, 3]);
		assert_eq!(
			iter(&reordered).collect::<Vec<_>>(),
			[entries[9], entries[0], DictionaryEntryId::default(), entries[3]]
		);
		let sliced = slice(&array, 7, 9);
		assert_eq!(iter(&sliced).collect::<Vec<_>>(), &entries[7..9]);
		assert_eq!(iter(&slice(&array, 8, 100)).collect::<Vec<_>>(), &entries[8..]);
		assert_eq!(slice(&array, 20, 30).len(), 0);
		assert_eq!(iter(&take(&array, 3)).collect::<Vec<_>>(), &entries[..3]);
		assert_eq!(
			iter(&filter(&sliced, &BooleanBuffer::from(vec![false, true]))).collect::<Vec<_>>(),
			[entries[8]]
		);
	}

	#[test]
	fn serde_writes_entries_like_a_slice_of_entry_ids() {
		// The column bytes must match the entry list encoding, or stored frames stop decoding.
		let entries = boundaries();
		let column = DictionaryColumn(dictionary_array(entries.clone()));
		let bytes = postcard::to_allocvec(&column).unwrap();
		assert_eq!(bytes, postcard::to_allocvec(&entries).unwrap());
		let back: DictionaryColumn = postcard::from_bytes(&bytes).unwrap();
		assert_eq!(iter(&back.0).collect::<Vec<_>>(), entries);
		let json = serde_json::to_string(&column).unwrap();
		assert_eq!(json, serde_json::to_string(&entries).unwrap());
		let back: DictionaryColumn = serde_json::from_str(&json).unwrap();
		assert_eq!(iter(&back.0).collect::<Vec<_>>(), entries);
	}

	#[test]
	fn sliced_array_serializes_only_its_rows() {
		// Serializing the whole backing buffer would leak rows outside the slice.
		let entries = boundaries();
		let sliced = slice(&dictionary_array(entries.clone()), 2, 4);
		let bytes = postcard::to_allocvec(&DictionaryColumn(sliced)).unwrap();
		assert_eq!(bytes, postcard::to_allocvec(&entries[2..4]).unwrap());
	}
}
