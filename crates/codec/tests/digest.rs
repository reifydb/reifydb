// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use reifydb_codec::{
	frame::{decode::decode_frames, encode::encode_frames, format::Encoding, options::EncodeOptions},
	json::{
		from::frames_from_json,
		to::frames_to_json,
		wire_type::{from_json, to_json},
	},
	key::serializer::KeySerializer,
	reader::Reader,
	tag::ValueKind,
	typeinfo::{decode_value_type, encode_value_type},
	value::{decode_value, encode_value},
};
use reifydb_value::value::{
	Value,
	container::digest_array::digest_array,
	digest::Digest,
	duration::Duration,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	value_type::ValueType,
};
use serde_json::json;

const ACCURACY: u32 = 10_000;

const SUPPORTED_INNER: [ValueType; 15] = [
	ValueType::Float4,
	ValueType::Float8,
	ValueType::Int1,
	ValueType::Int2,
	ValueType::Int4,
	ValueType::Int8,
	ValueType::Int16,
	ValueType::Uint1,
	ValueType::Uint2,
	ValueType::Uint4,
	ValueType::Uint8,
	ValueType::Uint16,
	ValueType::Duration,
	ValueType::INT,
	ValueType::UINT,
];

fn digest_type(inner: ValueType, accuracy: u32) -> ValueType {
	ValueType::Digest {
		inner: Box::new(inner),
		accuracy,
	}
}

fn float_digest(values: &[f64]) -> Digest {
	let mut digest = Digest::new(ValueType::Float8, ACCURACY).unwrap();
	for value in values {
		digest.add_value(&Value::float8(*value)).unwrap();
	}
	digest
}

fn duration_digest(millis: &[i64]) -> Digest {
	let mut digest = Digest::new(ValueType::Duration, ACCURACY).unwrap();
	for ms in millis {
		digest.add_value(&Value::Duration(Duration::from_milliseconds(*ms).unwrap())).unwrap();
	}
	digest
}

fn digest_column(rows: Vec<Option<Digest>>, inner: ValueType) -> FrameColumnData {
	let defined: Vec<bool> = rows.iter().map(Option::is_some).collect();
	let data = FrameColumnData::Digest {
		container: digest_array(rows),
		inner,
		accuracy: ACCURACY,
	};
	if defined.iter().all(|d| *d) {
		data
	} else {
		FrameColumnData::Option {
			inner: Box::new(data),
			bitvec: BooleanBuffer::from(defined.as_slice()),
		}
	}
}

fn frame(data: FrameColumnData) -> Frame {
	Frame::new(vec![FrameColumn {
		name: "d".to_string(),
		data,
	}])
}

fn sample_rows() -> Vec<Option<Digest>> {
	vec![
		Some(float_digest(&[1.0, 2.0, 3.0])),
		None,
		Some(float_digest(&[])),
		Some(float_digest(&[-5.0, 0.0, f64::INFINITY, 1e9])),
	]
}

fn assert_same_cells(expected: &FrameColumnData, actual: &FrameColumnData) {
	assert_eq!(expected.len(), actual.len(), "row count changed");
	for row in 0..expected.len() {
		assert_eq!(expected.get_value(row), actual.get_value(row), "row {row} changed");
	}
}

fn find(haystack: &[u8], needle: &[u8]) -> Vec<usize> {
	haystack.windows(needle.len()).enumerate().filter(|(_, w)| *w == needle).map(|(i, _)| i).collect()
}

#[test]
fn digest_kind_is_appended_last_as_tag_32() {
	// Persisted and wire tags must never shift, so a new kind only ever takes the next free number.
	assert_eq!(ValueKind::Digest.byte(), 32);
	assert_eq!(ValueKind::ALL.len(), 33);
	assert_eq!(ValueKind::ALL[32], ValueKind::Digest);
	assert_eq!(ValueKind::from_byte(32), Some(ValueKind::Digest));
	assert_eq!(ValueKind::from_byte(33), None);
	assert_eq!(ValueKind::of_type(&digest_type(ValueType::Float8, ACCURACY)), ValueKind::Digest);
}

#[test]
fn digest_inner_tag_equals_value_kind_for_every_supported_inner_type() {
	// The digest bytes and the typeinfo both name the inner type; a drift between the two tables corrupts decode.
	for inner in SUPPORTED_INNER {
		let bytes = Digest::new(inner.clone(), ACCURACY).unwrap().encode();
		assert_eq!(bytes[1], ValueKind::of_type(&inner).byte(), "digest inner tag for {inner}");
	}
}

#[test]
fn typeinfo_carries_inner_kind_and_accuracy_ppm_little_endian() {
	// The TypeScript decoder reads exactly these bytes, so the layout is pinned byte for byte.
	let mut buf = Vec::new();
	encode_value_type(&digest_type(ValueType::Duration, 12_345), &mut buf).unwrap();
	assert_eq!(buf, vec![32, 18, 0x39, 0x30, 0, 0]);

	let mut buf = Vec::new();
	encode_value_type(&ValueType::Option(Box::new(digest_type(ValueType::Float8, ACCURACY))), &mut buf).unwrap();
	assert_eq!(buf, vec![(1 << 6) | 32, 3, 0x10, 0x27, 0, 0]);
}

#[test]
fn typeinfo_round_trips_every_inner_type_under_option_layers() {
	// Depth 4 goes through the extended tag, which must still reach the digest params.
	for inner in SUPPORTED_INNER {
		for depth in [0, 1, 3, 4] {
			let ty = (0..depth)
				.fold(digest_type(inner.clone(), 1_000), |ty, _| ValueType::Option(Box::new(ty)));
			let mut buf = Vec::new();
			encode_value_type(&ty, &mut buf).unwrap();
			let mut reader = Reader::new(&buf);
			assert_eq!(decode_value_type(&mut reader).unwrap(), ty, "{ty} did not survive typeinfo");
		}
	}
}

#[test]
fn typeinfo_rejects_an_unsupported_inner_type_or_accuracy_in_both_directions() {
	// A digest type that cannot be built must never be written or accepted from the wire.
	let mut buf = Vec::new();
	assert!(encode_value_type(&digest_type(ValueType::Utf8, ACCURACY), &mut buf).is_err());
	assert!(encode_value_type(&digest_type(ValueType::Float8, 999), &mut Vec::new()).is_err());

	let utf8_inner = [32, 9, 0x10, 0x27, 0, 0];
	assert!(decode_value_type(&mut Reader::new(&utf8_inner)).is_err());
	let accuracy_too_fine = [32, 3, 0xE7, 0x03, 0, 0];
	assert!(decode_value_type(&mut Reader::new(&accuracy_too_fine)).is_err());
	let truncated = [32, 3, 0x10];
	assert!(decode_value_type(&mut Reader::new(&truncated)).is_err());
}

#[test]
fn value_codec_round_trips_a_digest_and_a_typed_none_of_digest() {
	let digest = Value::Digest(Box::new(float_digest(&[1.0, 2.0, 3.0])));
	assert_eq!(decode_value(&encode_value(&digest).unwrap()).unwrap(), digest);

	let duration = Value::Digest(Box::new(duration_digest(&[5, 10, 250])));
	assert_eq!(decode_value(&encode_value(&duration).unwrap()).unwrap(), duration);

	// A none keeps its digest type, so the inner type and accuracy survive with no digest present.
	let none = Value::none_of(digest_type(ValueType::Duration, ACCURACY));
	assert_eq!(decode_value(&encode_value(&none).unwrap()).unwrap(), none);
}

#[test]
fn frame_round_trip_keeps_digests_nones_and_column_params_for_every_compression() {
	// Every compression level ends in plain for a digest; none of them may drop a row or the params.
	let data = digest_column(sample_rows(), ValueType::Float8);
	for options in [
		EncodeOptions::default(),
		EncodeOptions::none(),
		EncodeOptions::fast(),
		EncodeOptions::max(),
		EncodeOptions::forced(Encoding::Dict),
		EncodeOptions::forced(Encoding::Rle),
	] {
		let bytes = encode_frames(&[frame(data.clone())], &options).unwrap();
		let decoded = decode_frames(&bytes).unwrap();
		let column = &decoded[0].columns[0].data;
		assert_same_cells(&data, column);
		let FrameColumnData::Option {
			inner,
			bitvec,
		} = column
		else {
			panic!("a digest column with a none row must decode as an option, got {:?}", column.get_type());
		};
		assert_eq!(bitvec.iter().collect::<Vec<_>>(), vec![true, false, true, true]);
		let FrameColumnData::Digest {
			inner: decoded_inner,
			accuracy,
			..
		} = inner.as_ref()
		else {
			panic!("the inner column must stay a digest column");
		};
		assert_eq!(*decoded_inner, ValueType::Float8);
		assert_eq!(*accuracy, ACCURACY);
	}
}

#[test]
fn frame_round_trip_keeps_a_duration_digest_column_without_nones() {
	let data = digest_column(
		vec![Some(duration_digest(&[1, 2, 3])), Some(duration_digest(&[-7, 90_000]))],
		ValueType::Duration,
	);
	let bytes = encode_frames(&[frame(data.clone())], &EncodeOptions::default()).unwrap();
	let decoded = decode_frames(&bytes).unwrap();
	assert_same_cells(&data, &decoded[0].columns[0].data);
	assert_eq!(decoded[0].columns[0].data.get_type(), digest_type(ValueType::Duration, ACCURACY));
}

#[test]
fn a_decoded_frame_renders_a_digest_as_its_count() {
	// Rendering must show the count only, never the raw buckets.
	let data = digest_column(vec![Some(float_digest(&[1.0, 2.0, 3.0])), None], ValueType::Float8);
	let bytes = encode_frames(&[frame(data)], &EncodeOptions::default()).unwrap();
	let rendered = decode_frames(&bytes).unwrap()[0].to_string();
	assert!(rendered.contains("digest(n: 3)"), "rendered frame lacks the digest count:\n{rendered}");
	assert!(rendered.contains("none"), "rendered frame lacks the none row:\n{rendered}");
}

#[test]
fn frame_decode_rejects_a_row_digest_whose_params_differ_from_the_column() {
	// A row digest with another accuracy would merge wrongly later, so decode must refuse it.
	let data = digest_column(vec![Some(float_digest(&[1.0]))], ValueType::Float8);
	let mut bytes = encode_frames(&[frame(data)], &EncodeOptions::default()).unwrap();
	let column_params = [3, 0x10, 0x27, 0, 0];
	let at = find(&bytes, &column_params);
	assert_eq!(at.len(), 1, "column params must appear exactly once in the frame");
	bytes[at[0] + 1..at[0] + 5].copy_from_slice(&20_000u32.to_le_bytes());
	assert!(decode_frames(&bytes).is_err(), "a mismatched row digest was accepted");
}

#[test]
fn frame_decode_rejects_digest_column_params_that_are_not_a_valid_digest_type() {
	let data = digest_column(vec![Some(float_digest(&[1.0]))], ValueType::Float8);
	let mut bytes = encode_frames(&[frame(data)], &EncodeOptions::default()).unwrap();
	let at = find(&bytes, &[3, 0x10, 0x27, 0, 0]);
	assert_eq!(at.len(), 1);
	// Inner kind 9 is utf8, which a digest cannot hold.
	bytes[at[0]] = 9;
	assert!(decode_frames(&bytes).is_err(), "a utf8 digest column was accepted");
}

#[test]
fn json_round_trip_keeps_digests_nones_and_column_params() {
	let data = digest_column(sample_rows(), ValueType::Float8);
	let json = frames_to_json(&[frame(data.clone())]).unwrap();
	let decoded = frames_from_json(&json).unwrap();
	let column = &decoded[0].columns[0].data;
	assert_same_cells(&data, column);
	assert_eq!(column.get_type(), data.get_type());
}

#[test]
fn json_wire_type_names_digest_with_inner_type_and_accuracy() {
	let ty = digest_type(ValueType::Duration, ACCURACY);
	let rendered = to_json(&ty);
	assert_eq!(rendered, json!({"id": "Digest", "underlying": {"id": "Duration"}, "accuracy": 10000}));
	assert_eq!(from_json(&rendered), Ok(ty));

	let option = ValueType::Option(Box::new(digest_type(ValueType::Int4, 1_000)));
	assert_eq!(from_json(&to_json(&option)), Ok(option));
}

#[test]
fn json_wire_type_rejects_a_digest_without_valid_params() {
	// Without accuracy the reader cannot rebuild the type, so it must fail rather than guess one.
	assert!(from_json(&json!({"id": "Digest", "underlying": {"id": "Float8"}})).is_err());
	assert!(from_json(&json!({"id": "Digest", "underlying": {"id": "Float8"}, "accuracy": -1})).is_err());
	assert!(from_json(&json!({"id": "Digest", "underlying": {"id": "Float8"}, "accuracy": 5_000_000_000u64}))
		.is_err());
	assert!(from_json(&json!({"id": "Digest", "underlying": {"id": "Utf8"}, "accuracy": 10000})).is_err());
	assert!(from_json(&json!({"id": "Digest", "underlying": {"id": "Float8"}, "accuracy": 500_000})).is_err());
}

#[test]
fn key_serializer_returns_an_error_for_a_digest_instead_of_panicking() {
	// A digest has no order, so it must never become part of a key.
	let mut serializer = KeySerializer::new();
	let err = serializer.try_extend_value(&Value::Digest(Box::new(float_digest(&[1.0])))).err().unwrap();
	assert_eq!(err.diagnostic().code, "SERDE_003");

	let mut serializer = KeySerializer::new();
	let none = Value::none_of(digest_type(ValueType::Float8, ACCURACY));
	let err = serializer.try_extend_value(&none).err().unwrap();
	assert_eq!(err.diagnostic().code, "SERDE_003");

	let mut serializer = KeySerializer::new();
	let nested = Value::List(vec![Value::Int4(1), Value::Digest(Box::new(float_digest(&[1.0])))]);
	assert!(serializer.try_extend_value(&nested).is_err(), "a digest inside a list reached the key");
}
