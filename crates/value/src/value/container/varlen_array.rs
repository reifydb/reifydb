// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Cow, result::Result as StdResult, str};

use arrow_array::{
	Array, GenericByteArray, LargeBinaryArray, LargeStringArray,
	builder::{GenericByteBuilder, LargeBinaryBuilder, LargeStringBuilder},
	types::ByteArrayType,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer};
use serde::{Deserialize, Deserializer, Serializer, de::Error as DeError, ser::SerializeSeq};
use serde_bytes::{ByteBuf, Bytes};

use crate::{
	util::bitmap,
	value::{Value, blob::Blob, value_type::ValueType},
};

pub fn get<T>(array: &GenericByteArray<T>, index: usize) -> Option<&T::Native>
where
	T: ByteArrayType<Offset = i64>,
{
	if index < array.len() {
		Some(array.value(index))
	} else {
		None
	}
}

fn row_bytes<T>(array: &GenericByteArray<T>, index: usize) -> &[u8]
where
	T: ByteArrayType<Offset = i64>,
{
	<T::Native as AsRef<[u8]>>::as_ref(array.value(index))
}

pub fn equals<T>(left: &GenericByteArray<T>, right: &GenericByteArray<T>) -> bool
where
	T: ByteArrayType<Offset = i64>,
{
	left.len() == right.len() && (0..left.len()).all(|i| row_bytes(left, i) == row_bytes(right, i))
}

pub fn empty<T>() -> GenericByteArray<T>
where
	T: ByteArrayType<Offset = i64>,
{
	GenericByteArray::new(OffsetBuffer::new_empty(), Buffer::from_vec(Vec::<u8>::new()), None)
}

fn data_byte_len<T>(array: &GenericByteArray<T>) -> usize
where
	T: ByteArrayType<Offset = i64>,
{
	let offsets = array.value_offsets();
	(offsets[offsets.len() - 1] - offsets[0]) as usize
}

pub fn compact_parts<T>(array: &GenericByteArray<T>) -> (&[u8], Cow<'_, [i64]>)
where
	T: ByteArrayType<Offset = i64>,
{
	let offsets = array.value_offsets();
	let first = offsets[0];
	let last = offsets[offsets.len() - 1];
	let data = &array.value_data()[first as usize..last as usize];
	let offsets = if first == 0 {
		Cow::Borrowed(offsets)
	} else {
		Cow::Owned(offsets.iter().map(|offset| offset - first).collect())
	};
	(data, offsets)
}

pub fn slice<T>(array: &GenericByteArray<T>, start: usize, end: usize) -> GenericByteArray<T>
where
	T: ByteArrayType<Offset = i64>,
{
	let len = array.len();
	let start = start.min(len);
	let end = end.min(len);
	if start >= end {
		return attach_nulls(empty(), bitmap::slice_nulls(array.nulls(), start, end));
	}
	array.slice(start, end - start)
}

pub fn take<T>(array: &GenericByteArray<T>, num: usize) -> GenericByteArray<T>
where
	T: ByteArrayType<Offset = i64>,
{
	slice(array, 0, num)
}

pub fn filter<T>(array: &GenericByteArray<T>, mask: &BooleanBuffer) -> GenericByteArray<T>
where
	T: ByteArrayType<Offset = i64>,
{
	let mut builder = GenericByteBuilder::<T>::with_capacity(array.len(), data_byte_len(array));
	for (i, keep) in mask.iter().enumerate() {
		if keep && i < array.len() {
			builder.append_value(array.value(i));
		}
	}
	attach_nulls(builder.finish(), bitmap::filter_nulls(array.nulls(), mask))
}

pub fn reorder<T>(array: &GenericByteArray<T>, indices: &[usize]) -> GenericByteArray<T>
where
	T: ByteArrayType<Offset = i64>,
	for<'a> &'a T::Native: Default,
{
	let mut builder = GenericByteBuilder::<T>::with_capacity(indices.len(), data_byte_len(array));
	for &idx in indices {
		match get(array, idx) {
			Some(value) => builder.append_value(value),
			None => builder.append_value(<&T::Native>::default()),
		}
	}
	attach_nulls(builder.finish(), bitmap::reorder_nulls(array.nulls(), indices))
}

pub fn attach_nulls<T>(array: GenericByteArray<T>, nulls: Option<NullBuffer>) -> GenericByteArray<T>
where
	T: ByteArrayType<Offset = i64>,
{
	bitmap::assert_nulls_len(nulls.as_ref(), array.len());
	let (offsets, values, _) = array.into_parts();
	// SAFETY: offsets and values come from a valid array of this type, and the validity length matches its rows.
	unsafe { GenericByteArray::new_unchecked(offsets, values, nulls) }
}

pub fn capacity<T>(array: &GenericByteArray<T>) -> usize
where
	T: ByteArrayType<Offset = i64>,
{
	let offsets = array.offsets().inner().inner();
	if offsets.strong_count() == 1 {
		(offsets.capacity() / size_of::<i64>()).saturating_sub(1)
	} else {
		array.len()
	}
}

pub fn heap_size<T>(array: &GenericByteArray<T>) -> usize
where
	T: ByteArrayType<Offset = i64>,
{
	let values = array.values();
	let data = if values.strong_count() == 1 {
		values.capacity()
	} else {
		data_byte_len(array)
	};
	data + (capacity(array) + 1) * size_of::<i64>()
}

pub fn blob_array(values: &[Blob]) -> LargeBinaryArray {
	LargeBinaryArray::from_iter_values(values.iter().map(|blob| blob.as_bytes()))
}

pub fn utf8_get_value(array: &LargeStringArray, index: usize) -> Value {
	match get(array, index) {
		Some(s) => Value::Utf8(s.to_string()),
		None => Value::none_of(ValueType::Utf8),
	}
}

pub fn utf8_as_string(array: &LargeStringArray, index: usize) -> String {
	match get(array, index) {
		Some(s) => s.to_string(),
		None => "none".to_string(),
	}
}

pub fn blob_get_value(array: &LargeBinaryArray, index: usize) -> Value {
	match get(array, index) {
		Some(bytes) => Value::Blob(Blob::new(bytes.to_vec())),
		None => Value::none_of(ValueType::Blob),
	}
}

pub fn blob_as_string(array: &LargeBinaryArray, index: usize) -> String {
	match get(array, index) {
		Some(bytes) => Blob::new(bytes.to_vec()).to_string(),
		None => "none".to_string(),
	}
}

pub fn serialize<T, Ser>(array: &GenericByteArray<T>, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error>
where
	T: ByteArrayType<Offset = i64>,
	Ser: Serializer,
{
	let mut seq = serializer.serialize_seq(Some(array.len()))?;
	for i in 0..array.len() {
		seq.serialize_element(Bytes::new(row_bytes(array, i)))?;
	}
	seq.end()
}

pub fn deserialize_utf8<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<LargeStringArray, D::Error> {
	let items: Vec<ByteBuf> = Vec::deserialize(deserializer)?;
	let total: usize = items.iter().map(|b| b.len()).sum();
	let mut builder = LargeStringBuilder::with_capacity(items.len(), total);
	for item in &items {
		let text = str::from_utf8(item.as_slice()).map_err(DeError::custom)?;
		builder.append_value(text);
	}
	Ok(builder.finish())
}

pub fn deserialize_blob<'de, D: Deserializer<'de>>(deserializer: D) -> StdResult<LargeBinaryArray, D::Error> {
	let items: Vec<ByteBuf> = Vec::deserialize(deserializer)?;
	let total: usize = items.iter().map(|b| b.len()).sum();
	let mut builder = LargeBinaryBuilder::with_capacity(items.len(), total);
	for item in &items {
		builder.append_value(item.as_slice());
	}
	Ok(builder.finish())
}

#[cfg(test)]
mod tests {
	use postcard::{from_bytes as postcard_from_bytes, to_allocvec as postcard_to_allocvec};
	use serde::{Deserialize, Serialize};

	use super::*;

	#[derive(Serialize)]
	struct Utf8Column(#[serde(serialize_with = "serialize")] LargeStringArray);

	#[derive(Serialize, Deserialize)]
	struct BlobColumn(
		#[serde(serialize_with = "serialize", deserialize_with = "deserialize_blob")] LargeBinaryArray,
	);

	mod utf8 {
		use super::*;

		#[test]
		fn test_slice() {
			// slice must return exactly the rows in start..end.
			let container = LargeStringArray::from(vec![
				"one".to_string(),
				"two".to_string(),
				"three".to_string(),
				"four".to_string(),
			]);
			let sliced = slice(&container, 1, 3);

			assert_eq!(sliced.len(), 2);
			assert_eq!(get(&sliced, 0), Some("two"));
			assert_eq!(get(&sliced, 1), Some("three"));
		}

		#[test]
		fn test_filter() {
			// filter must keep exactly the rows whose mask bit is set, in order.
			let container = LargeStringArray::from(vec![
				"keep".to_string(),
				"drop".to_string(),
				"keep".to_string(),
				"drop".to_string(),
			]);
			let mask = BooleanBuffer::from(vec![true, false, true, false]);

			let container = filter(&container, &mask);

			assert_eq!(container.len(), 2);
			assert_eq!(get(&container, 0), Some("keep"));
			assert_eq!(get(&container, 1), Some("keep"));
		}

		#[test]
		fn test_reorder() {
			// reorder must place row indices[i] at position i.
			let container = LargeStringArray::from(vec![
				"first".to_string(),
				"second".to_string(),
				"third".to_string(),
			]);
			let indices = [2, 0, 1];

			let container = reorder(&container, &indices);

			assert_eq!(container.len(), 3);
			assert_eq!(get(&container, 0), Some("third"));
			assert_eq!(get(&container, 1), Some("first"));
			assert_eq!(get(&container, 2), Some("second"));
		}

		#[test]
		fn test_reorder_with_out_of_bounds() {
			// An out of range index must yield an empty row, never panic or shift the other rows.
			let container = LargeStringArray::from(vec!["a".to_string(), "b".to_string()]);
			let indices = [1, 5, 0];

			let container = reorder(&container, &indices);

			assert_eq!(container.len(), 3);
			assert_eq!(get(&container, 0), Some("b"));
			assert_eq!(get(&container, 1), Some(""));
			assert_eq!(get(&container, 2), Some("a"));
		}

		#[test]
		fn test_empty_strings() {
			// An empty row must read back as a defined empty string, never as a missing row.
			let mut builder = LargeStringBuilder::with_capacity(2, 0);
			builder.append_value("".to_string());
			builder.append_value("");
			let container = builder.finish();

			assert_eq!(container.len(), 2);
			assert_eq!(get(&container, 0), Some(""));
			assert_eq!(get(&container, 1), Some(""));

			assert!(0 < container.len());
			assert!(1 < container.len());
		}

		#[test]
		fn test_data_bytes_and_offsets_match_zero_copy_layout() {
			// compact_parts must hand out exactly the row bytes and offsets starting at zero.
			let container = LargeStringArray::from(vec!["aa".to_string(), "bb".to_string()]);
			assert_eq!(compact_parts(&container).0, b"aabb");
			assert_eq!(&*compact_parts(&container).1, &[0i64, 2, 4]);
		}

		#[test]
		fn test_postcard_wire_compat() {
			// Postcard bytes must match Vec<String>, otherwise stored state and CDC stop decoding.
			let strings = vec!["hello".to_string(), "world".to_string()];
			let strings_bytes: Vec<u8> = postcard_to_allocvec(&strings).unwrap();

			let container = Utf8Column(LargeStringArray::from(strings.clone()));
			let container_bytes: Vec<u8> = postcard_to_allocvec(&container).unwrap();

			assert_eq!(strings_bytes, container_bytes);
		}
	}

	mod blob {
		use super::*;

		#[test]
		fn test_data_bytes_and_offsets_match_zero_copy_layout() {
			// compact_parts must hand out exactly the row bytes and offsets starting at zero.
			let container = blob_array(&[Blob::new(vec![0xAA, 0xBB]), Blob::new(vec![0xCC])]);
			assert_eq!(compact_parts(&container).0, &[0xAAu8, 0xBB, 0xCC]);
			assert_eq!(&*compact_parts(&container).1, &[0i64, 2, 3]);
		}

		#[test]
		fn test_postcard_wire_compat() {
			// Postcard bytes must match Vec<Blob>, otherwise stored state and CDC stop decoding.
			let blobs = vec![Blob::new(vec![1, 2, 3]), Blob::new(vec![4, 5])];
			let blobs_bytes: Vec<u8> = postcard_to_allocvec(&blobs).unwrap();

			let container = BlobColumn(blob_array(&blobs));
			let container_bytes: Vec<u8> = postcard_to_allocvec(&container).unwrap();

			assert_eq!(blobs_bytes, container_bytes);
		}
	}

	mod varlen {
		use super::*;

		fn abcd() -> LargeBinaryArray {
			LargeBinaryArray::from_iter_values([b"aa".as_slice(), b"bb", b"cc", b"dd"])
		}

		#[test]
		fn filter_in_place_keeps_matching_elements() {
			// filter must keep exactly the rows whose mask bit is set, in order.
			let c = LargeBinaryArray::from_iter_values([b"yes".as_slice(), b"no", b"yes", b"no"]);
			let c = filter(&c, &BooleanBuffer::collect_bool(4, |i| i % 2 == 0));
			assert_eq!(c.len(), 2);
			assert_eq!(get(&c, 0), Some(b"yes".as_slice()));
			assert_eq!(get(&c, 1), Some(b"yes".as_slice()));
		}

		#[test]
		fn reorder_in_place_handles_oob_as_empty() {
			// An out of range index must yield an empty row, never panic or shift the other rows.
			let c = LargeBinaryArray::from_iter_values([b"a".as_slice(), b"b"]);
			let c = reorder(&c, &[1, 100, 0]);
			assert_eq!(c.len(), 3);
			assert_eq!(get(&c, 0), Some(b"b".as_slice()));
			assert_eq!(get(&c, 1), Some(b"".as_slice()));
			assert_eq!(get(&c, 2), Some(b"a".as_slice()));
		}

		#[test]
		fn take_n_truncates() {
			// take must keep exactly the first n rows.
			let c = LargeBinaryArray::from_iter_values([b"a".as_slice(), b"b", b"c"]);
			let t = take(&c, 2);
			assert_eq!(t.len(), 2);
			assert_eq!(get(&t, 0), Some(b"a".as_slice()));
			assert_eq!(get(&t, 1), Some(b"b".as_slice()));
		}

		#[test]
		fn slice_extracts_subrange_with_rebased_offsets() {
			// A slice must read its own rows and hand out offsets rebased exactly to zero.
			let c = LargeBinaryArray::from_iter_values([b"aa".as_slice(), b"bb", b"cc", b"dd"]);
			let s = slice(&c, 1, 3);
			assert_eq!(s.len(), 2);
			assert_eq!(get(&s, 0), Some(b"bb".as_slice()));
			assert_eq!(get(&s, 1), Some(b"cc".as_slice()));
			assert_eq!(&*compact_parts(&s).1, &[0i64, 2, 4]);
		}

		#[test]
		fn serde_round_trip_preserves_content() {
			// A postcard round trip must keep every row, including empty ones, in order.
			let original =
				BlobColumn(LargeBinaryArray::from_iter_values([b"hello".as_slice(), b"", b"world"]));
			let encoded: Vec<u8> = postcard_to_allocvec(&original).unwrap();
			let decoded: BlobColumn = postcard_from_bytes(&encoded).unwrap();
			let decoded = decoded.0;
			assert_eq!(decoded.len(), 3);
			assert_eq!(get(&decoded, 0), Some(b"hello".as_slice()));
			assert_eq!(get(&decoded, 1), Some(b"".as_slice()));
			assert_eq!(get(&decoded, 2), Some(b"world".as_slice()));
		}

		#[test]
		fn serde_wire_compat_with_vec_of_strings() {
			// Byte rows must decode Vec<String> bytes, otherwise rows written as strings stop decoding.
			let strings = vec!["a".to_string(), "bc".to_string(), "def".to_string()];
			let encoded: Vec<u8> = postcard_to_allocvec(&strings).unwrap();
			let decoded: BlobColumn = postcard_from_bytes(&encoded).unwrap();
			let decoded = decoded.0;
			assert_eq!(decoded.len(), 3);
			assert_eq!(get(&decoded, 0), Some(b"a".as_slice()));
			assert_eq!(get(&decoded, 1), Some(b"bc".as_slice()));
			assert_eq!(get(&decoded, 2), Some(b"def".as_slice()));
		}

		#[test]
		fn equality_compares_logical_content() {
			// Two arrays with the same rows must compare equal.
			let a = LargeBinaryArray::from_iter_values([b"x".as_slice(), b"y"]);
			let b = LargeBinaryArray::from_iter_values([b"x".as_slice(), b"y"]);
			assert!(equals(&a, &b));
		}

		#[test]
		fn frozen_slice_compact_parts_are_rebased() {
			// A guest must see only the referenced bytes and a first offset of 0, never the parent buffer.
			let c = abcd();
			let s = slice(&c, 1, 3);
			let (data, offsets) = compact_parts(&s);
			assert_eq!(data, b"bbcc");
			assert_eq!(&*offsets, &[0i64, 2, 4]);
			assert!(matches!(offsets, Cow::Owned(_)));
		}

		#[test]
		fn unsliced_compact_parts_borrow_offsets() {
			// Unsliced containers must stay zero-copy across the ABI.
			let c = abcd();
			let (data, offsets) = compact_parts(&c);
			assert_eq!(data, b"aabbccdd");
			assert_eq!(&*offsets, &[0i64, 2, 4, 6, 8]);
			assert!(matches!(offsets, Cow::Borrowed(_)));
		}

		#[test]
		fn empty_range_slice_of_frozen_container_is_empty() {
			// An empty range must yield len 0 and no bytes, even when the clamp lands mid-buffer.
			let c = abcd();
			for (start, end) in [(2, 2), (3, 1), (10, 20)] {
				let s = slice(&c, start, end);
				assert_eq!(s.len(), 0);
				assert_eq!(data_byte_len(&s), 0);
				assert_eq!(compact_parts(&s).0, b"");
			}
		}

		#[test]
		fn filter_on_frozen_slice_keeps_right_rows() {
			// filter must read rows through absolute offsets, not assume the buffer starts at the slice.
			let s = slice(&abcd(), 1, 4);
			let s = filter(&s, &BooleanBuffer::collect_bool(3, |i| i != 1));
			assert_eq!(s.len(), 2);
			assert_eq!(get(&s, 0), Some(b"bb".as_slice()));
			assert_eq!(get(&s, 1), Some(b"dd".as_slice()));
			assert_eq!(s.value_data(), b"bbdd");
			assert_eq!(s.value_offsets(), &[0i64, 2, 4]);
		}
	}
}
