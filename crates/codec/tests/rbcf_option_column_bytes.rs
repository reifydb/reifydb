// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Write as _, sync::Arc};

use arrow_array::{Array, ArrayRef, Int32Array, LargeStringArray, RecordBatch, make_array};
use arrow_buffer::NullBuffer;
use arrow_schema::Schema;
use reifydb_codec::frame::{
	decode::decode_frames,
	encode::encode_frames,
	format::{Encoding, FRAME_HEADER_SIZE, MESSAGE_HEADER_SIZE},
	options::EncodeOptions,
};
use reifydb_value::value::{
	Value,
	column_view::ColumnView,
	container::{any_array::any_array_optional, dictionary_array::dictionary_array, digest_array::digest_array},
	dictionary::DictionaryEntryId,
	digest::Digest,
	frame::frame::Frame,
	value_type::{ValueType, field::named},
};

const ENCODING_AT: usize = MESSAGE_HEADER_SIZE + FRAME_HEADER_SIZE + 1;

struct Pin {
	name: &'static str,
	encoding: Encoding,
	bytes: &'static str,
}

const PINS: &[Pin] = &[
	Pin {
		name: "plain_option_int4",
		encoding: Encoding::Plain,
		bytes: "52424346010000000100000049000000030000000100000039000000460001000100000003000000010000000c00000000000000000000006300000005010000006300000003000000",
	},
	Pin {
		name: "plain_option_int4_zero_nones",
		encoding: Encoding::Plain,
		bytes: "52424346010000000100000049000000030000000100000039000000460001000100000003000000010000000c00000000000000000000006300000007010000000200000003000000",
	},
	Pin {
		name: "plain_option_int4_bitmap_at_a_bit_offset",
		encoding: Encoding::Plain,
		bytes: "524243460100000001000000620000000900000001000000520000004600010001000000090000000200000024000000000000000000000063000000b601030000000400000005000000060000000700000008000000090000000a0000000b000000",
	},
	Pin {
		name: "dict_option_utf8",
		encoding: Encoding::Dict,
		bytes: "5242434601000000010000005b00000008000000010000004b0000004901010001000000080000000100000008000000000000001600000063000000b7000100020100020003000000000000000100000002000000020000007879",
	},
	Pin {
		name: "rle_option_int4",
		encoding: Encoding::Rle,
		bytes: "524243460100000001000000550000000800000001000000450000004602010001000000080000000100000018000000000000000000000063000000e7050000000300000000000000020000000700000003000000",
	},
	Pin {
		name: "delta_option_int4",
		encoding: Encoding::Delta,
		bytes: "52424346010000000100000049000000080000000100000039000000460301000100000008000000010000000c000000000000000000000063000000f7010a0000000101f40e010101",
	},
	Pin {
		name: "plain_option_any",
		encoding: Encoding::Plain,
		bytes: "5242434601000000010000004a00000003000000010000003a000000580001000100000003000000010000000d0000000000000000000000630000000506090000000018090100000078",
	},
	Pin {
		name: "plain_option_digest",
		encoding: Encoding::Plain,
		bytes: "5242434601000000010000006b00000003000000010000005b0000005e00010001000000030000000100000019000000100000000500000063000000050103904e000000000200012e010103904e000000018c010100000000000d0000000d000000190000000310270000",
	},
	Pin {
		name: "plain_option_dictionary_id",
		encoding: Encoding::Plain,
		bytes: "5242434601000000010000004100000003000000010000003100000059000100010000000300000001000000040000000000000000000000630000000501030009",
	},
];

const REJECTED_PINS: &[(&str, usize, &str)] = &[
	(
		"plain_option_option_int4",
		2,
		"5242434601000000010000004a00000003000000010000003a000000860001000100000003000000020000000c0000000000000000000000630000000305070000000000000009000000",
	),
	(
		"plain_option_option_option_int4",
		3,
		"5242434601000000010000004f00000004000000010000003f000000c600010001000000040000000300000010000000000000000000000063000000070b0d01000000000000000000000004000000",
	),
];

type Column = (ValueType, ArrayRef);

fn option((value_type, array): Column, defined: &[bool]) -> Column {
	let data = array.to_data().into_builder().nulls(Some(NullBuffer::from(defined))).build().unwrap();
	(ValueType::Option(Box::new(value_type)), make_array(data))
}

fn int4(values: &[i32]) -> Column {
	(ValueType::Int4, Arc::new(Int32Array::from(values.to_vec())))
}

fn float_digest(values: &[f64]) -> Digest {
	let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
	for value in values {
		digest.add_value(&Value::float8(*value)).unwrap();
	}
	digest
}

fn option_int4_bitmap_at_a_bit_offset() -> Column {
	let values: Vec<i32> = (0..16).collect();
	let defined: Vec<bool> = (0..16).map(|i| !(3..12).contains(&i) || i % 3 != 0).collect();
	let (value_type, array) = option((ValueType::Int4, Arc::new(Int32Array::from(values))), &defined);
	(value_type, array.slice(3, 9))
}

fn fixtures() -> Vec<(&'static str, EncodeOptions, Column)> {
	vec![
		("plain_option_int4", EncodeOptions::none(), option(int4(&[1, 99, 3]), &[true, false, true])),
		("plain_option_int4_zero_nones", EncodeOptions::none(), option(int4(&[1, 2, 3]), &[true, true, true])),
		(
			"plain_option_int4_bitmap_at_a_bit_offset",
			EncodeOptions::none(),
			option_int4_bitmap_at_a_bit_offset(),
		),
		(
			"dict_option_utf8",
			EncodeOptions::forced(Encoding::Dict),
			option(
				(
					ValueType::Utf8,
					Arc::new(LargeStringArray::from(vec!["x", "y", "x", "", "y", "x", "", "x"])),
				),
				&[true, true, true, false, true, true, false, true],
			),
		),
		(
			"rle_option_int4",
			EncodeOptions::forced(Encoding::Rle),
			option(int4(&[5, 5, 5, 0, 0, 7, 7, 7]), &[true, true, true, false, false, true, true, true]),
		),
		(
			"delta_option_int4",
			EncodeOptions::forced(Encoding::Delta),
			option(
				int4(&[10, 11, 12, 0, 14, 15, 16, 17]),
				&[true, true, true, false, true, true, true, true],
			),
		),
		(
			"plain_option_any",
			EncodeOptions::none(),
			option(
				(
					ValueType::Any,
					Arc::new(any_array_optional([
						Some(Value::Int4(9)),
						None,
						Some(Value::Utf8("x".to_string())),
					])),
				),
				&[true, false, true],
			),
		),
		(
			"plain_option_digest",
			EncodeOptions::none(),
			option(
				(
					ValueType::Digest {
						inner: Box::new(ValueType::Float8),
						accuracy: 10_000,
					},
					Arc::new(digest_array(vec![
						Some(float_digest(&[1.0, 2.5])),
						None,
						Some(float_digest(&[-4.0])),
					])),
				),
				&[true, false, true],
			),
		),
		(
			"plain_option_dictionary_id",
			EncodeOptions::none(),
			option(
				(
					ValueType::DictionaryId,
					Arc::new(dictionary_array(vec![
						DictionaryEntryId::U1(3),
						DictionaryEntryId::U1(0),
						DictionaryEntryId::U1(9),
					])),
				),
				&[true, false, true],
			),
		),
	]
}

fn frame((value_type, array): Column) -> Frame {
	let (field, array) = named("c", value_type.into(), array);
	Frame::from(RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap())
}

fn view(frame: &Frame) -> ColumnView<'_> {
	ColumnView::try_from((frame.batch.column(0), frame.batch.schema_ref().field(0))).unwrap()
}

fn names(frame: &Frame) -> Vec<String> {
	frame.batch.schema_ref().fields().iter().map(|field| field.name().clone()).collect()
}

fn cells(array: &ArrayRef) -> (Vec<bool>, ArrayRef) {
	let defined = (0..array.len()).map(|row| array.is_valid(row)).collect();
	(defined, make_array(array.to_data().into_builder().nulls(None).build().unwrap()))
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

fn pin(name: &str) -> &'static Pin {
	PINS.iter().find(|pin| pin.name == name).unwrap_or_else(|| panic!("no pin for fixture {name}"))
}

fn decode_single(name: &str, bytes: &[u8]) -> Result<Frame, String> {
	let mut frames = decode_frames(bytes).map_err(|err| format!("{name}: decode failed: {err}"))?;
	if frames.len() != 1 {
		return Err(format!("{name}: decoded {} frames, expected exactly one", frames.len()));
	}
	Ok(frames.remove(0))
}

fn report(mismatches: Vec<String>) {
	assert!(mismatches.is_empty(), "RBCF output drifted from the pins:\n{}", mismatches.join("\n"));
}

#[test]
fn every_fixture_has_exactly_one_pin() {
	// A fixture that loses its pin would stop guarding its wire bytes without any test going red.
	let fixture_names: Vec<&str> = fixtures().iter().map(|(name, _, _)| *name).collect();
	let pin_names: Vec<&str> = PINS.iter().map(|pin| pin.name).collect();
	assert_eq!(fixture_names, pin_names);
}

#[test]
fn option_columns_encode_to_the_pinned_rbcf_bytes() {
	// RBCF reaches TypeScript clients and older peers, so every byte must stay exactly as pinned.
	let mismatches = fixtures()
		.into_iter()
		.flat_map(|(name, options, data)| {
			let pin = pin(name);
			let bytes = encode_frames(&[frame(data)], &options).unwrap();
			let actual = hex(&bytes);
			[
				(actual != pin.bytes).then(|| format!("{name} bytes: \"{actual}\"")),
				(bytes[ENCODING_AT] != pin.encoding as u8).then(|| {
					format!(
						"{name}: encoding byte {} but the pin expects {:?}",
						bytes[ENCODING_AT], pin.encoding
					)
				}),
			]
		})
		.flatten()
		.collect();
	report(mismatches);
}

#[test]
fn pinned_rbcf_bytes_decode_to_the_fixture() {
	// Bytes a previous build wrote must read back with the same layers, bits and placeholders under none rows.
	let mismatches = fixtures()
		.into_iter()
		.filter_map(|(name, _, expected)| {
			let expected = frame(expected);
			match decode_single(name, &unhex(pin(name).bytes)) {
				Err(err) => Some(err),
				Ok(decoded) if names(&decoded) != ["c"] => {
					Some(format!("{name}: decoded columns {:?}", names(&decoded)))
				}
				Ok(decoded) if view(&decoded).get_type() != view(&expected).get_type() => {
					Some(format!(
						"{name}: decoded type {:?}, expected {:?}",
						view(&decoded).get_type(),
						view(&expected).get_type()
					))
				}
				Ok(decoded) if cells(decoded.batch.column(0)) != cells(expected.batch.column(0)) => {
					Some(format!(
						"{name}: decoded {:?}, expected {:?}",
						decoded.batch.column(0),
						expected.batch.column(0)
					))
				}
				Ok(_) => None,
			}
		})
		.collect();
	report(mismatches);
}

#[test]
fn decoded_option_columns_re_encode_to_the_pinned_bytes() {
	// Re-encoding a decoded frame must give exactly the pinned bytes, otherwise relays alter frames.
	let mismatches = fixtures()
		.into_iter()
		.filter_map(|(name, options, _)| {
			let pinned = pin(name).bytes;
			match decode_single(name, &unhex(pinned)) {
				Err(err) => Some(err),
				Ok(decoded) => {
					let actual = hex(&encode_frames(&[decoded], &options).unwrap());
					(actual != pinned).then(|| format!("{name} re-encoded: \"{actual}\""))
				}
			}
		})
		.collect();
	report(mismatches);
}

#[test]
fn nested_option_pins_are_rejected_on_decode() {
	// A column holds at most one option layer, so depth two or three bytes must never decode.
	for (name, depth, bytes) in REJECTED_PINS {
		let error = decode_single(name, &unhex(bytes)).map(|frame| frame.batch.num_rows()).unwrap_err();
		let expected = format!("has option depth {depth}, but a column holds at most one option layer");
		assert!(error.contains(&expected), "{error}");
	}
}
