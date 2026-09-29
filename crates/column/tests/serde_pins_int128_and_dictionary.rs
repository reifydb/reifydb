// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Write as _, sync::Arc};

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use postcard::{from_bytes, to_stdvec};
use reifydb_column::{
	encoding::{Encoding, canonical::CanonicalEncoding},
	persist::PersistedArray,
};
use reifydb_core::value::column::{
	data::{Column, canonical::Canonical},
	factory,
};
use reifydb_value::value::{
	dictionary::{DictionaryEntryId, DictionaryId},
	value_type::{
		ValueType,
		field::{from_field, named},
	},
};

fn hex(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		write!(out, "{byte:02x}").unwrap();
	}
	out
}

fn unhex(text: &str) -> Vec<u8> {
	(0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap()).collect()
}

fn chunk_hex(canonical: &Canonical) -> String {
	let persisted = CanonicalEncoding::FIXED.persist(&Column::from_canonical(canonical.clone())).unwrap();
	hex(&to_stdvec(&persisted).unwrap())
}

fn decode_chunk(pinned: &str, ty: &ValueType) -> Result<Arc<Canonical>, String> {
	let persisted: PersistedArray = from_bytes(&unhex(pinned)).map_err(|err| err.to_string())?;
	let column = CanonicalEncoding::FIXED.load(persisted, ty).map_err(|err| err.to_string())?;
	column.to_canonical().map_err(|err| err.to_string())
}

fn with_dictionary_id(column: (FieldRef, ArrayRef), id: DictionaryId) -> (FieldRef, ArrayRef) {
	let mut field_type = from_field(&column.0).unwrap();
	assert_eq!(
		field_type.value_type,
		Some(ValueType::DictionaryId),
		"dictionary_id factory must build a DictionaryId column"
	);
	field_type.dictionary_id = Some(id);
	named(column.0.name(), field_type, column.1)
}

fn sliced(column: (FieldRef, ArrayRef), start: usize, end: usize) -> (FieldRef, ArrayRef) {
	(column.0, column.1.slice(start, end - start))
}

fn assert_pinned(column: (FieldRef, ArrayRef), pinned: &str) {
	let expected = Canonical::from_column(&column).unwrap();
	let written = chunk_hex(&expected);
	let mismatches: Vec<String> = [
		(written != pinned).then(|| format!("chunk_postcard: \"{written}\"")),
		match decode_chunk(pinned, &expected.view().base_type()) {
			Ok(decoded)
				if decoded.field_type() == expected.field_type()
					&& decoded.buffer() == expected.buffer() =>
			{
				None
			}
			Ok(decoded) => Some(format!("chunk postcard: decoded {decoded:?}, expected {expected:?}")),
			Err(err) => Some(format!("chunk postcard: decode failed: {err}")),
		},
	]
	.into_iter()
	.flatten()
	.collect();
	assert!(mismatches.is_empty(), "persisted chunk bytes drifted from the pin:\n{}", mismatches.join("\n"));
}

#[test]
fn dictionary_id_with_some_dictionary_id_is_pinned() {
	// A set dictionary_id must be written and read back, otherwise a stored column loses its dictionary.
	let column = with_dictionary_id(
		factory::dictionary_id("c", [DictionaryEntryId::U4(1), DictionaryEntryId::U4(2)]),
		DictionaryId(42),
	);
	assert_pinned(column, "00011900012a0018020201020200");
}

#[test]
fn dictionary_id_u1_rows_are_pinned() {
	// A U1 row must keep its width tag and one-byte value, otherwise the row codec reads a different id.
	let column = factory::dictionary_id("c", [DictionaryEntryId::U1(1), DictionaryEntryId::U1(u8::MAX)]);
	assert_pinned(column, "0001190000001802000100ff00");
}

#[test]
fn dictionary_id_u2_rows_are_pinned() {
	// A U2 row must keep its width tag and varint value, otherwise the row codec reads a different id.
	let column = factory::dictionary_id("c", [DictionaryEntryId::U2(1), DictionaryEntryId::U2(u16::MAX)]);
	assert_pinned(column, "0001190000001802010101ffff0300");
}

#[test]
fn dictionary_id_u8_rows_are_pinned() {
	// A U8 row must keep its width tag and varint value, otherwise the row codec reads a different id.
	let column = factory::dictionary_id("c", [DictionaryEntryId::U8(1), DictionaryEntryId::U8(u64::MAX)]);
	assert_pinned(column, "0001190000001802030103ffffffffffffffffff0100");
}

#[test]
fn dictionary_id_mixed_width_rows_are_pinned() {
	// Each row must keep its own width, otherwise a fixed-width codec widens or narrows rows on the wire.
	let column = factory::dictionary_id(
		"c",
		[
			DictionaryEntryId::U1(3),
			DictionaryEntryId::U4(7),
			DictionaryEntryId::U8(9),
			DictionaryEntryId::U16(1),
		],
	);
	assert_pinned(column, "0001190000001804000302070309040100");
}

#[test]
fn dictionary_id_u1_zero_placeholder_rows_are_pinned() {
	// Placeholder rows must stay U1(0) on the wire, otherwise a none row decodes to a real dictionary entry.
	let column = factory::dictionary_id(
		"c",
		[DictionaryEntryId::U1(0), DictionaryEntryId::U1(0), DictionaryEntryId::U1(0)],
	);
	assert_pinned(column, "000119000000180300000000000000");
}

#[test]
fn sliced_int16_is_pinned() {
	// A sliced Int16 must serialize like a fresh column, otherwise the wire format depends on slicing.
	let column = sliced(factory::int16("c", [i128::MIN, -2, i128::MAX, 2, 0]), 1, 4);
	assert_pinned(column, "000107000000070303feffffffffffffffffffffffffffffffffff030400");
}

#[test]
fn sliced_uint16_is_pinned() {
	// A sliced Uint16 must serialize like a fresh column, otherwise the wire format depends on slicing.
	let column = sliced(factory::uint16("c", [0, 1, u128::MAX, 3, 4]), 1, 4);
	assert_pinned(column, "00010d0000000c0301ffffffffffffffffffffffffffffffffffff030300");
}

#[test]
fn sliced_dictionary_id_is_pinned() {
	// A sliced DictionaryId must serialize like a fresh column, otherwise the wire format depends on slicing.
	let column = factory::dictionary_id(
		"c",
		[
			DictionaryEntryId::U4(1),
			DictionaryEntryId::U4(2),
			DictionaryEntryId::U4(3),
			DictionaryEntryId::U4(4),
			DictionaryEntryId::U4(5),
		],
	);
	assert_pinned(sliced(column, 1, 4), "000119000000180302020203020400");
}

#[test]
fn sliced_dictionary_id_keeps_some_dictionary_id_is_pinned() {
	// A slice must keep the dictionary_id, otherwise a sliced column is written without its dictionary.
	let column = with_dictionary_id(
		factory::dictionary_id(
			"c",
			[
				DictionaryEntryId::U4(1),
				DictionaryEntryId::U4(2),
				DictionaryEntryId::U4(3),
				DictionaryEntryId::U4(4),
				DictionaryEntryId::U4(5),
			],
		),
		DictionaryId(42),
	);
	assert_pinned(sliced(column, 1, 4), "00011900012a00180302020203020400");
}

#[test]
fn option_int16_with_none_row_is_pinned() {
	// The none row must keep its default value and cleared bit, otherwise optional Int16 columns drift.
	let column = factory::int16_with_bitvec("c", [i128::MIN, 0, i128::MAX], vec![true, false, true]);
	assert_pinned(
		column,
		"000117070000000703ffffffffffffffffffffffffffffffffffff0300feffffffffffffffffffffffffffffffffff0301010503",
	);
}

#[test]
fn option_uint16_with_none_row_is_pinned() {
	// The none row must keep its default value and cleared bit, otherwise optional Uint16 columns drift.
	let column = factory::uint16_with_bitvec("c", [1, 0, u128::MAX], vec![true, false, true]);
	assert_pinned(column, "0001170d0000000c030100ffffffffffffffffffffffffffffffffffff0301010503");
}

#[test]
fn option_dictionary_id_with_none_row_is_pinned() {
	// The none row must stay a U1(0) placeholder with a cleared bit, otherwise optional DictionaryId columns drift.
	let column = factory::dictionary_id_with_bitvec(
		"c",
		[DictionaryEntryId::U4(7), DictionaryEntryId::default(), DictionaryEntryId::U8(9)],
		vec![true, false, true],
	);
	assert_pinned(column, "00011719000000180302070000030901010503");
}
