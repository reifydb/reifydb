// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::str::FromStr;

use arrow_buffer::BooleanBuffer;
use reifydb_codec::{
	constraint::{EncodedTypeConstraint, decode_type_constraint, encode_type_constraint},
	extern_c::cells::{decode_decimal_cell, encode_decimal_cell},
	frame::{decode::decode_frames, encode::encode_frames, options::EncodeOptions},
	json::{from::frames_from_json, to::frames_to_json},
	reader::Reader,
	row::shape::{RowFamily, RowShape},
	tag::ValueKind,
	typeinfo::{decode_value_type, encode_value_type},
};
use reifydb_value::value::{
	Value,
	constraint::{Constraint, TypeConstraint, bytes::MaxBytes, precision::Precision, scale::Scale},
	container::decimal_array::decimal_array,
	decimal::Decimal,
	frame::{column::FrameColumn, data::FrameColumnData, frame::Frame},
	value_type::ValueType,
};

fn p(value: u8) -> Precision {
	Precision::new(value)
}

fn s(value: u8) -> Scale {
	Scale::new(value)
}

fn option(inner: ValueType) -> ValueType {
	ValueType::Option(Box::new(inner))
}

fn family_types() -> Vec<ValueType> {
	vec![
		ValueType::decimal(p(1), s(0)),
		ValueType::decimal(p(10), s(2)),
		ValueType::decimal(p(38), s(38)),
		ValueType::decimal(p(39), s(0)),
		ValueType::DECIMAL,
		ValueType::decimal(p(76), s(76)),
	]
}

fn decimal(text: &str) -> Decimal {
	Decimal::from_str(text).unwrap()
}

#[test]
fn typeinfo_carries_precision_and_scale_at_every_option_depth() {
	for base in family_types() {
		for depth in [0u8, 1, 3, 5] {
			// Deep options take the extended tag path, which must not drop the params either.
			let ty = (0..depth).fold(base.clone(), |ty, _| option(ty));
			let mut buf = Vec::new();
			encode_value_type(&ty, &mut buf).unwrap();
			let mut reader = Reader::new(&buf);
			assert_eq!(decode_value_type(&mut reader).unwrap(), ty, "{ty}");
			assert!(reader.is_empty(), "{ty} left trailing bytes");
		}
	}
}

#[test]
fn typeinfo_rejects_params_no_layout_can_hold() {
	// A corrupt precision or scale byte must fail decode rather than build a type with no storage width.
	let mut dec = Vec::new();
	encode_value_type(&ValueType::decimal(p(5), s(2)), &mut dec).unwrap();
	*dec.last_mut().unwrap() = 6;
	assert!(decode_value_type(&mut Reader::new(&dec)).is_err(), "scale past precision");
}

#[test]
fn a_family_type_encodes_as_constraint_code_two_with_its_params() {
	// Code 2 was PrecisionScale, so decimal(p, s) keeps the fingerprint catalogs already stored for it.
	let encoded = encode_type_constraint(&TypeConstraint::unconstrained(ValueType::decimal(p(10), s(2)))).unwrap();
	assert_eq!(encoded.base_type, ValueKind::Decimal.byte());
	assert_eq!((encoded.constraint_type, encoded.constraint_param1, encoded.constraint_param2), (2, 10, 2));
}

#[test]
fn a_family_type_constraint_round_trips_including_options() {
	for base in family_types() {
		for ty in [base.clone(), option(base.clone()), option(option(base))] {
			// An option around the family must keep the params, or a nullable column decodes at the
			// defaults.
			let tc = TypeConstraint::unconstrained(ty.clone());
			let decoded = decode_type_constraint(&encode_type_constraint(&tc).unwrap()).unwrap();
			assert_eq!(decoded.get_type(), ty, "{ty}");
		}
	}
}

#[test]
fn a_family_constraint_with_impossible_params_is_rejected() {
	// Scale on an int, or a precision past 76, has no storage layout and must not decode.
	let wide = EncodedTypeConstraint {
		base_type: ValueKind::Decimal.byte(),
		constraint_type: 2,
		constraint_param1: 300,
		constraint_param2: 0,
	};
	assert!(decode_type_constraint(&wide).is_err());
}

#[test]
fn a_family_type_with_an_extra_constraint_is_refused_on_encode() {
	// The params occupy the constraint slot, so a second constraint would be silently dropped.
	let tc =
		TypeConstraint::with_constraint(ValueType::decimal(p(5), s(2)), Constraint::MaxBytes(MaxBytes::new(4)));
	assert!(encode_type_constraint(&tc).is_err());
}

#[test]
fn extern_c_cells_are_fixed_width_and_round_trip_at_both_widths() {
	for (precision, width) in [(38u8, 16usize), (76, 32)] {
		for text in ["0", "-1.5", "12345.67"] {
			let mut buf = Vec::new();
			encode_decimal_cell(&decimal(text), p(precision), s(2), &mut buf).unwrap();
			assert_eq!(buf.len(), width);
			let read = decode_decimal_cell(&buf, s(2)).unwrap();
			assert_eq!(read, decimal(text));
			assert_eq!(read.scale(), 2);
		}
	}
}

#[test]
fn extern_c_cells_refuse_values_and_buffers_that_do_not_fit() {
	// Truncating on encode or reading a short buffer would hand the guest a different number.
	let mut buf = Vec::new();
	assert!(encode_decimal_cell(&decimal("1.25"), p(10), s(1), &mut buf).is_err());
	assert!(buf.is_empty(), "a refused cell must not leave partial bytes");
	assert!(decode_decimal_cell(&[0u8; 33], s(0)).is_err());
}

#[test]
fn row_values_round_trip_at_both_slot_widths() {
	let types = vec![ValueType::decimal(p(38), s(4)), ValueType::decimal(p(76), s(4))];
	let shape = RowShape::testing(RowFamily::Pod, &types);
	let values = vec![
		Value::Decimal(decimal("-1234.5")),
		Value::Decimal(decimal(&format!("{}.{}", "9".repeat(72), "9".repeat(4)))),
	];
	let mut row = shape.allocate_pod();
	shape.set_values(&mut row, &values);
	// Slots never spill, so the row must stay at its static size whatever the magnitude.
	assert_eq!(row.len(), shape.total_static_size());
	for (index, expected) in values.iter().enumerate() {
		let actual = shape.get_value(&row, index);
		assert_eq!(&actual, expected, "field {index}");
		if let Value::Decimal(read) = actual {
			assert_eq!(read.scale(), 4, "field {index} must read back at the column scale");
		}
	}
	for index in 0..types.len() {
		shape.set_value(&mut row, index, &Value::none_of(types[index].clone()));
		assert!(!row.is_defined(index));
	}
}

#[test]
fn json_frames_keep_precision_scale_and_values() {
	let frame = Frame::new(vec![
		FrameColumn {
			name: "d".to_string(),
			data: FrameColumnData::Decimal(decimal_array(
				p(10),
				s(2),
				[decimal("1.5"), decimal("-99999999.99")],
			)),
		},
		FrameColumn {
			name: "od".to_string(),
			data: FrameColumnData::Option {
				inner: Box::new(FrameColumnData::Decimal(decimal_array(
					p(50),
					s(3),
					[decimal("0.001"), decimal("0")],
				))),
				bitvec: BooleanBuffer::from(vec![true, false]),
			},
		},
	]);
	// The text form alone loses the column type, so the wire type must carry the params back.
	let decoded = frames_from_json(&frames_to_json(&[frame.clone()]).unwrap()).unwrap();
	assert_eq!(decoded.len(), 1);
	for (want, got) in frame.columns.iter().zip(&decoded[0].columns) {
		assert_eq!(want.name, got.name);
		assert_eq!(want.data.get_type(), got.data.get_type(), "{}", want.name);
		for row in 0..want.data.len() {
			let (a, b) = (want.data.get_value(row), got.data.get_value(row));
			assert_eq!(a, b, "{} row {row}", want.name);
			if let (Value::Decimal(a), Value::Decimal(b)) = (&a, &b) {
				assert_eq!(a.scale(), b.scale(), "{} row {row}", want.name);
			}
		}
	}
}

fn decimal5_2_frame_bytes() -> Vec<u8> {
	let frame = Frame::new(vec![FrameColumn {
		name: "n".to_string(),
		data: FrameColumnData::Decimal(decimal_array(p(5), s(2), [decimal("123.45")])),
	}]);
	encode_frames(&[frame], &EncodeOptions::none()).unwrap()
}

fn patched_decode_error(patch: impl Fn(&mut Vec<u8>)) -> String {
	let mut bytes = decimal5_2_frame_bytes();
	patch(&mut bytes);
	let error = decode_frames(&bytes).unwrap_err();
	format!("{error} {error:?}")
}

fn params_position(bytes: &[u8]) -> usize {
	bytes.windows(2).rposition(|window| window == [5, 2]).expect("the decimal(5, 2) column carries [5, 2] as extra")
}

fn descriptor_position(bytes: &[u8]) -> usize {
	let descriptor = [ValueKind::Decimal.byte(), 0, 0, 0, 1, 0];
	bytes.windows(descriptor.len())
		.position(|window| window == descriptor)
		.expect("the plain decimal column descriptor")
}

#[test]
fn a_frame_column_whose_values_exceed_its_precision_is_rejected() {
	// Narrowing the declared precision below the data must fail decode instead of building an invalid array.
	let error = patched_decode_error(|bytes| {
		let at = params_position(bytes);
		bytes[at] = 4;
	});
	assert!(error.contains("more digits than precision"), "{error}");
}

#[test]
fn a_frame_decimal_column_with_a_scale_past_its_precision_is_rejected() {
	// A scale above the precision has no valid layout, so the header must fail decode instead of reading the data.
	let error = patched_decode_error(|bytes| {
		let at = params_position(bytes);
		bytes[at + 1] = 6;
	});
	assert!(error.contains("invalid scale 6"), "{error}");
}

#[test]
fn a_frame_family_column_under_dict_encoding_is_rejected() {
	// The family has no dict layout, so reading the data as dict indices would invent values.
	let error = patched_decode_error(|bytes| {
		let at = descriptor_position(bytes);
		bytes[at + 1] = 1;
	});
	assert!(error.contains("Dict encoding not supported"), "{error}");
}
