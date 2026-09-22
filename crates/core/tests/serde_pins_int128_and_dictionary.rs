// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{Debug, Write as _};

use postcard::{from_bytes, to_stdvec};
use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_value::value::{
	dictionary::{DictionaryEntryId, DictionaryId},
	frame::data::FrameColumnData,
};
use serde::{Serialize, de::DeserializeOwned};

struct Pin {
	column_postcard: &'static str,
	column_json: &'static str,
	frame_postcard: &'static str,
	frame_json: &'static str,
}

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

fn postcard_hex<T: Serialize>(value: &T) -> String {
	hex(&to_stdvec(value).unwrap())
}

fn json<T: Serialize>(value: &T) -> String {
	serde_json::to_string(value).unwrap()
}

fn decode_postcard<T: DeserializeOwned>(pinned: &str) -> Result<T, String> {
	from_bytes(&unhex(pinned)).map_err(|err| err.to_string())
}

fn decode_json<T: DeserializeOwned>(pinned: &str) -> Result<T, String> {
	serde_json::from_str(pinned).map_err(|err| err.to_string())
}

fn check_decoded<T: PartialEq + Debug>(label: &str, decoded: Result<T, String>, expected: &T) -> Option<String> {
	match decoded {
		Ok(value) if value == *expected => None,
		Ok(value) => Some(format!("{label}: decoded {value:?}, expected {expected:?}")),
		Err(err) => Some(format!("{label}: decode failed: {err}")),
	}
}

fn with_dictionary_id(mut buffer: ColumnBuffer, id: DictionaryId) -> ColumnBuffer {
	let ColumnBuffer::DictionaryId {
		dictionary_id,
		..
	} = &mut buffer
	else {
		panic!("dictionary_id factory must build a DictionaryId buffer");
	};
	*dictionary_id = Some(id);
	buffer
}

fn assert_pinned(buffer: ColumnBuffer, pin: Pin) {
	let frame = FrameColumnData::from(buffer.clone());
	let column_postcard = postcard_hex(&buffer);
	let column_json = json(&buffer);
	let frame_postcard = postcard_hex(&frame);
	let frame_json = json(&frame);
	let mismatches: Vec<String> = [
		(column_postcard != pin.column_postcard).then(|| format!("column_postcard: \"{column_postcard}\"")),
		(column_json != pin.column_json).then(|| format!("column_json: {column_json:?}")),
		(frame_postcard != pin.frame_postcard).then(|| format!("frame_postcard: \"{frame_postcard}\"")),
		(frame_json != pin.frame_json).then(|| format!("frame_json: {frame_json:?}")),
		check_decoded("column postcard", decode_postcard(pin.column_postcard), &buffer),
		check_decoded("column json", decode_json(pin.column_json), &buffer),
		check_decoded("frame postcard", decode_postcard(pin.frame_postcard), &frame),
		check_decoded("frame json", decode_json(pin.frame_json), &frame),
	]
	.into_iter()
	.flatten()
	.collect();
	assert!(mismatches.is_empty(), "serde output drifted from the pins:\n{}", mismatches.join("\n"));
}

#[test]
fn dictionary_id_with_some_dictionary_id_is_pinned() {
	// A set dictionary_id must be written and read back, otherwise a stored column loses its dictionary.
	let buffer = with_dictionary_id(
		ColumnBuffer::dictionary_id([DictionaryEntryId::U4(1), DictionaryEntryId::U4(2)]),
		DictionaryId(42),
	);
	assert_pinned(
		buffer,
		Pin {
			column_postcard: "1a0202010202012a",
			column_json: "{\"DictionaryId\":{\"data\":[{\"U4\":1},{\"U4\":2}],\"dictionary_id\":42}}",
			frame_postcard: "1a0202010202012a",
			frame_json: "{\"DictionaryId\":{\"data\":[{\"U4\":1},{\"U4\":2}],\"dictionary_id\":42}}",
		},
	);
}

#[test]
fn dictionary_id_u1_rows_are_pinned() {
	// A U1 row must keep its width tag and one-byte value, otherwise the row codec reads a different id.
	let buffer = ColumnBuffer::dictionary_id([DictionaryEntryId::U1(1), DictionaryEntryId::U1(u8::MAX)]);
	assert_pinned(
		buffer,
		Pin {
			column_postcard: "1a02000100ff00",
			column_json: "{\"DictionaryId\":{\"data\":[{\"U1\":1},{\"U1\":255}],\"dictionary_id\":null}}",
			frame_postcard: "1a02000100ff00",
			frame_json: "{\"DictionaryId\":{\"data\":[{\"U1\":1},{\"U1\":255}],\"dictionary_id\":null}}",
		},
	);
}

#[test]
fn dictionary_id_u2_rows_are_pinned() {
	// A U2 row must keep its width tag and varint value, otherwise the row codec reads a different id.
	let buffer = ColumnBuffer::dictionary_id([DictionaryEntryId::U2(1), DictionaryEntryId::U2(u16::MAX)]);
	assert_pinned(
		buffer,
		Pin {
			column_postcard: "1a02010101ffff0300",
			column_json: "{\"DictionaryId\":{\"data\":[{\"U2\":1},{\"U2\":65535}],\"dictionary_id\":null}}",
			frame_postcard: "1a02010101ffff0300",
			frame_json: "{\"DictionaryId\":{\"data\":[{\"U2\":1},{\"U2\":65535}],\"dictionary_id\":null}}",
		},
	);
}

#[test]
fn dictionary_id_u8_rows_are_pinned() {
	// A U8 row must keep its width tag and varint value, otherwise the row codec reads a different id.
	let buffer = ColumnBuffer::dictionary_id([DictionaryEntryId::U8(1), DictionaryEntryId::U8(u64::MAX)]);
	assert_pinned(
		buffer,
		Pin {
			column_postcard: "1a02030103ffffffffffffffffff0100",
			column_json: "{\"DictionaryId\":{\"data\":[{\"U8\":1},{\"U8\":18446744073709551615}],\"dictionary_id\":null}}",
			frame_postcard: "1a02030103ffffffffffffffffff0100",
			frame_json: "{\"DictionaryId\":{\"data\":[{\"U8\":1},{\"U8\":18446744073709551615}],\"dictionary_id\":null}}",
		},
	);
}

#[test]
fn dictionary_id_mixed_width_rows_are_pinned() {
	// Each row must keep its own width, otherwise a fixed-width codec widens or narrows rows on the wire.
	let buffer = ColumnBuffer::dictionary_id([
		DictionaryEntryId::U1(3),
		DictionaryEntryId::U4(7),
		DictionaryEntryId::U8(9),
		DictionaryEntryId::U16(1),
	]);
	assert_pinned(
		buffer,
		Pin {
			column_postcard: "1a04000302070309040100",
			column_json: "{\"DictionaryId\":{\"data\":[{\"U1\":3},{\"U4\":7},{\"U8\":9},{\"U16\":1}],\"dictionary_id\":null}}",
			frame_postcard: "1a04000302070309040100",
			frame_json: "{\"DictionaryId\":{\"data\":[{\"U1\":3},{\"U4\":7},{\"U8\":9},{\"U16\":1}],\"dictionary_id\":null}}",
		},
	);
}

#[test]
fn dictionary_id_u1_zero_placeholder_rows_are_pinned() {
	// Placeholder rows must stay U1(0) on the wire, otherwise a none row decodes to a real dictionary entry.
	let buffer = ColumnBuffer::dictionary_id([
		DictionaryEntryId::U1(0),
		DictionaryEntryId::U1(0),
		DictionaryEntryId::U1(0),
	]);
	assert_pinned(
		buffer,
		Pin {
			column_postcard: "1a0300000000000000",
			column_json: "{\"DictionaryId\":{\"data\":[{\"U1\":0},{\"U1\":0},{\"U1\":0}],\"dictionary_id\":null}}",
			frame_postcard: "1a0300000000000000",
			frame_json: "{\"DictionaryId\":{\"data\":[{\"U1\":0},{\"U1\":0},{\"U1\":0}],\"dictionary_id\":null}}",
		},
	);
}

#[test]
fn sliced_int16_is_pinned() {
	// A sliced Int16 must serialize like a fresh column, otherwise the wire format depends on slicing.
	let buffer = ColumnBuffer::int16([i128::MIN, -2, i128::MAX, 2, 0]).slice(1, 4);
	assert_pinned(
		buffer,
		Pin {
			column_postcard: "070303feffffffffffffffffffffffffffffffffff0304",
			column_json: "{\"Int16\":{\"data\":[-2,170141183460469231731687303715884105727,2]}}",
			frame_postcard: "070303feffffffffffffffffffffffffffffffffff0304",
			frame_json: "{\"Int16\":{\"data\":[-2,170141183460469231731687303715884105727,2]}}",
		},
	);
}

#[test]
fn sliced_uint16_is_pinned() {
	// A sliced Uint16 must serialize like a fresh column, otherwise the wire format depends on slicing.
	let buffer = ColumnBuffer::uint16([0, 1, u128::MAX, 3, 4]).slice(1, 4);
	assert_pinned(
		buffer,
		Pin {
			column_postcard: "0c0301ffffffffffffffffffffffffffffffffffff0303",
			column_json: "{\"Uint16\":{\"data\":[1,340282366920938463463374607431768211455,3]}}",
			frame_postcard: "0c0301ffffffffffffffffffffffffffffffffffff0303",
			frame_json: "{\"Uint16\":{\"data\":[1,340282366920938463463374607431768211455,3]}}",
		},
	);
}

#[test]
fn sliced_dictionary_id_is_pinned() {
	// A sliced DictionaryId must serialize like a fresh column, otherwise the wire format depends on slicing.
	let buffer = ColumnBuffer::dictionary_id([
		DictionaryEntryId::U4(1),
		DictionaryEntryId::U4(2),
		DictionaryEntryId::U4(3),
		DictionaryEntryId::U4(4),
		DictionaryEntryId::U4(5),
	])
	.slice(1, 4);
	assert_pinned(
		buffer,
		Pin {
			column_postcard: "1a0302020203020400",
			column_json: "{\"DictionaryId\":{\"data\":[{\"U4\":2},{\"U4\":3},{\"U4\":4}],\"dictionary_id\":null}}",
			frame_postcard: "1a0302020203020400",
			frame_json: "{\"DictionaryId\":{\"data\":[{\"U4\":2},{\"U4\":3},{\"U4\":4}],\"dictionary_id\":null}}",
		},
	);
}

#[test]
fn sliced_dictionary_id_keeps_some_dictionary_id_is_pinned() {
	// A slice must keep the dictionary_id, otherwise a sliced column is written without its dictionary.
	let buffer = with_dictionary_id(
		ColumnBuffer::dictionary_id([
			DictionaryEntryId::U4(1),
			DictionaryEntryId::U4(2),
			DictionaryEntryId::U4(3),
			DictionaryEntryId::U4(4),
			DictionaryEntryId::U4(5),
		]),
		DictionaryId(42),
	)
	.slice(1, 4);
	assert_pinned(
		buffer,
		Pin {
			column_postcard: "1a03020202030204012a",
			column_json: "{\"DictionaryId\":{\"data\":[{\"U4\":2},{\"U4\":3},{\"U4\":4}],\"dictionary_id\":42}}",
			frame_postcard: "1a03020202030204012a",
			frame_json: "{\"DictionaryId\":{\"data\":[{\"U4\":2},{\"U4\":3},{\"U4\":4}],\"dictionary_id\":42}}",
		},
	);
}

#[test]
fn option_int16_with_none_row_is_pinned() {
	// The none row must keep its default value and cleared bit, otherwise optional Int16 columns drift.
	let buffer = ColumnBuffer::int16_optional([Some(i128::MIN), None, Some(i128::MAX)]);
	assert_pinned(
		buffer,
		Pin {
			column_postcard: "1b0703ffffffffffffffffffffffffffffffffffff0300feffffffffffffffffffffffffffffffffff03010503",
			column_json: "{\"Option\":{\"inner\":{\"Int16\":{\"data\":[-170141183460469231731687303715884105728,0,170141183460469231731687303715884105727]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
			frame_postcard: "1b0703ffffffffffffffffffffffffffffffffffff0300feffffffffffffffffffffffffffffffffff03010503",
			frame_json: "{\"Option\":{\"inner\":{\"Int16\":{\"data\":[-170141183460469231731687303715884105728,0,170141183460469231731687303715884105727]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		},
	);
}

#[test]
fn option_uint16_with_none_row_is_pinned() {
	// The none row must keep its default value and cleared bit, otherwise optional Uint16 columns drift.
	let buffer = ColumnBuffer::uint16_optional([Some(1), None, Some(u128::MAX)]);
	assert_pinned(
		buffer,
		Pin {
			column_postcard: "1b0c030100ffffffffffffffffffffffffffffffffffff03010503",
			column_json: "{\"Option\":{\"inner\":{\"Uint16\":{\"data\":[1,0,340282366920938463463374607431768211455]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
			frame_postcard: "1b0c030100ffffffffffffffffffffffffffffffffffff03010503",
			frame_json: "{\"Option\":{\"inner\":{\"Uint16\":{\"data\":[1,0,340282366920938463463374607431768211455]}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		},
	);
}

#[test]
fn option_dictionary_id_with_none_row_is_pinned() {
	// The none row must stay a U1(0) placeholder with a cleared bit, otherwise optional DictionaryId columns drift.
	let buffer = ColumnBuffer::dictionary_id_optional([
		Some(DictionaryEntryId::U4(7)),
		None,
		Some(DictionaryEntryId::U8(9)),
	]);
	assert_pinned(
		buffer,
		Pin {
			column_postcard: "1b1a0302070000030900010503",
			column_json: "{\"Option\":{\"inner\":{\"DictionaryId\":{\"data\":[{\"U4\":7},{\"U1\":0},{\"U8\":9}],\"dictionary_id\":null}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
			frame_postcard: "1b1a0302070000030900010503",
			frame_json: "{\"Option\":{\"inner\":{\"DictionaryId\":{\"data\":[{\"U4\":7},{\"U1\":0},{\"U8\":9}],\"dictionary_id\":null}},\"bitvec\":{\"bits\":[5],\"len\":3}}}",
		},
	);
}
