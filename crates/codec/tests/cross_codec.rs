// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Proves the tag namespace is actually single: the same value carried through the value codec, an
//! RBCF Any column, a row Any field, and a keycode key must present the same leading kind byte and
//! round-trip to the same value.

use reifydb_codec::{
	frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions},
	key::{deserializer::KeyDeserializer, serializer::KeySerializer},
	tag::ValueKind,
	value::encode_value,
};
use reifydb_value::value::{
	Value,
	blob::Blob,
	constraint::TypeConstraint,
	container::any::AnyContainer,
	date::Date,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	ordered_f64::OrderedF64,
	value_type::ValueType,
};

fn cross_codec_values() -> Vec<Value> {
	vec![
		Value::none_of(ValueType::Duration),
		Value::none_of(ValueType::Option(Box::new(ValueType::Duration))),
		Value::Boolean(true),
		Value::Int4(-7),
		Value::Int8(i64::MAX),
		Value::Utf8("cross".to_string()),
		Value::Float8(OrderedF64::try_from(1.5).unwrap()),
		Value::Date(Date::from_days_since_epoch(19_000).unwrap()),
		Value::Blob(Blob::new(vec![9, 8, 7])),
	]
}

fn value_codec_kind_byte(value: &Value) -> u8 {
	encode_value(value).unwrap()[0]
}

fn key_kind_byte(value: &Value) -> u8 {
	let mut ser = KeySerializer::new();
	ser.extend_value(value);
	ser.finish().as_slice()[0]
}

#[test]
fn kind_byte_is_identical_across_codecs() {
	for value in cross_codec_values() {
		let expected = ValueKind::of_value(&value).byte();
		assert_eq!(value_codec_kind_byte(&value), expected, "value codec kind byte for {value:?}");
		assert_eq!(key_kind_byte(&value), expected, "keycode kind byte for {value:?}");
	}
}

#[test]
fn value_codec_and_rbcf_any_column_round_trip_identically() {
	let values = cross_codec_values();
	let column = FrameColumn {
		name: "c".to_string(),
		data: FrameColumnData::Any(AnyContainer::new(values.iter().cloned().map(Box::new).collect())),
	};
	let frame = Frame::new(vec![column]);
	let bytes = encode_frames(&[frame], &EncodeOptions::fast()).unwrap();
	let decoded = decode_frames(&bytes).unwrap();
	match &decoded[0].columns[0].data {
		FrameColumnData::Any(container) => {
			for (expected, actual) in values.iter().zip(container.iter()) {
				match (expected, actual.as_ref()) {
					(
						Value::None {
							inner: l,
						},
						Value::None {
							inner: r,
						},
					) => assert_eq!(l, r, "none inner type through RBCF"),
					(l, r) => assert_eq!(l, r),
				}
			}
		}
		other => panic!("expected any column, got {other:?}"),
	}
}

#[test]
fn value_codec_and_row_any_field_round_trip_identically() {
	use reifydb_codec::encoded::shape::{RowShape, RowShapeField};

	let shape = RowShape::new(vec![RowShapeField::unconstrained("v", ValueType::Any)]);
	for value in cross_codec_values() {
		let mut row = shape.allocate();
		let wrapped = Value::Any(Box::new(value.clone()));
		shape.set_value(&mut row, 0, &wrapped);
		let read = shape.get_value(&row, 0);
		match (&wrapped, &read) {
			(Value::Any(l), Value::Any(r)) => match (l.as_ref(), r.as_ref()) {
				(
					Value::None {
						inner: li,
					},
					Value::None {
						inner: ri,
					},
				) => assert_eq!(li, ri, "none inner type through row any field"),
				(l, r) => assert_eq!(l, r),
			},
			_ => panic!("expected any value back, got {read:?}"),
		}
	}
}

#[test]
fn keycode_round_trips_the_same_values() {
	for value in cross_codec_values() {
		let mut ser = KeySerializer::new();
		ser.extend_value(&value);
		let key = ser.finish();
		let mut de = KeyDeserializer::from_bytes(&key);
		let read = de.read_value().unwrap();
		match (&value, &read) {
			(
				Value::None {
					inner: l,
				},
				Value::None {
					inner: r,
				},
			) => assert_eq!(l, r, "none inner type through keycode"),
			(l, r) => assert_eq!(l, r),
		}
		assert!(de.is_empty());
	}
}

#[test]
fn constraint_ffi_round_trips_base_types() {
	use reifydb_codec::constraint::{type_constraint_from_ffi, type_constraint_to_ffi};

	let types = [
		ValueType::Boolean,
		ValueType::Int4,
		ValueType::Utf8,
		ValueType::Option(Box::new(ValueType::Duration)),
		ValueType::Option(Box::new(ValueType::Option(Box::new(ValueType::Int8)))),
	];
	for ty in types {
		let tc = TypeConstraint::unconstrained(ty.clone());
		let ffi = type_constraint_to_ffi(&tc).unwrap();
		let back = type_constraint_from_ffi(&ffi).unwrap();
		assert_eq!(back.get_type(), ty, "constraint base type must round-trip");
	}
}
