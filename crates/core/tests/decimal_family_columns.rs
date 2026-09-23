// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use reifydb_codec::row::shape::{RowFamily, RowShape, RowShapeField};
use reifydb_core::{
	row::Row,
	value::column::{ColumnWithName, buffer::ColumnBuffer, builder::ColumnBuilder, columns::Columns},
};
use reifydb_value::value::{
	Value,
	constraint::{TypeConstraint, precision::Precision, scale::Scale},
	container::decimal_array::DecimalArray,
	decimal::Decimal,
	frame::data::FrameColumnData,
	int::Int,
	row_number::RowNumber,
	uint::Uint,
	value_type::ValueType,
};

fn family_array(buffer: &ColumnBuffer) -> &DecimalArray {
	match buffer {
		ColumnBuffer::Int(array) | ColumnBuffer::Uint(array) | ColumnBuffer::Decimal(array) => array,
		other => panic!("expected an int, uint or decimal column, got {:?}", other.get_type()),
	}
}

fn is_decimal256(buffer: &ColumnBuffer) -> bool {
	match family_array(buffer) {
		DecimalArray::Decimal128(_) => false,
		DecimalArray::Decimal256(_) => true,
	}
}

fn decimal(text: &str) -> Decimal {
	text.parse().unwrap()
}

fn family_types(precision: u8) -> [ValueType; 3] {
	let precision = Precision::new(precision);
	[ValueType::int(precision), ValueType::uint(precision), ValueType::decimal(precision, Scale::new(4))]
}

fn round_trips(buffer: &ColumnBuffer) -> Vec<ColumnBuffer> {
	let frame = FrameColumnData::from(buffer.clone());
	let frame_bytes = postcard::to_stdvec(&frame).unwrap();
	let frame_json = serde_json::to_string(&frame).unwrap();
	let column_bytes = postcard::to_stdvec(buffer).unwrap();
	let column_json = serde_json::to_string(buffer).unwrap();
	vec![
		ColumnBuffer::from(frame),
		ColumnBuffer::from(postcard::from_bytes::<FrameColumnData>(&frame_bytes).unwrap()),
		ColumnBuffer::from(serde_json::from_str::<FrameColumnData>(&frame_json).unwrap()),
		postcard::from_bytes::<ColumnBuffer>(&column_bytes).unwrap(),
		serde_json::from_str::<ColumnBuffer>(&column_json).unwrap(),
	]
}

fn sample_columns() -> [ColumnBuffer; 3] {
	[
		ColumnBuffer::int(Precision::new(20), [Int::from(-7i64), Int::from(i64::MAX)]),
		ColumnBuffer::uint(Precision::new(39), [Uint::from(7u64), Uint::from(u128::MAX)]),
		ColumnBuffer::decimal(Precision::new(12), Scale::new(3), [decimal("1.25"), decimal("-0.5")]),
	]
}

#[test]
fn precision_38_is_decimal128_for_every_family_type() {
	// Precision 38 must stay on the 16 byte layout, otherwise every narrow column doubles its memory.
	for ty in family_types(38) {
		assert!(!is_decimal256(&ColumnBuilder::with_capacity(ty.clone(), 1).finish()), "{ty:?} builder");
		assert!(!is_decimal256(&ColumnBuffer::none_typed(ty.clone(), 2)), "{ty:?} none typed");
		assert_eq!(ColumnBuffer::none_typed(ty.clone(), 2).get_type(), ValueType::Option(Box::new(ty)));
	}
	assert!(!is_decimal256(&ColumnBuffer::int(Precision::new(38), [Int::from(i64::MIN)])));
	assert!(!is_decimal256(&ColumnBuffer::uint(Precision::new(38), [Uint::from(u64::MAX)])));
	assert!(!is_decimal256(&ColumnBuffer::decimal(Precision::new(38), Scale::new(4), [decimal("1.5")])));
}

#[test]
fn precision_39_is_decimal256_for_every_family_type() {
	// Precision 39 needs more than 128 bits, otherwise a full width value overflows the native storage.
	for ty in family_types(39) {
		assert!(is_decimal256(&ColumnBuilder::with_capacity(ty.clone(), 1).finish()), "{ty:?} builder");
		assert!(is_decimal256(&ColumnBuffer::none_typed(ty.clone(), 2)), "{ty:?} none typed");
		assert_eq!(ColumnBuffer::none_typed(ty.clone(), 2).get_type(), ValueType::Option(Box::new(ty)));
	}
	assert!(is_decimal256(&ColumnBuffer::int(Precision::new(39), [Int::from(i64::MIN)])));
	assert!(is_decimal256(&ColumnBuffer::uint(Precision::new(39), [Uint::from(u128::MAX)])));
	assert!(is_decimal256(&ColumnBuffer::decimal(Precision::new(39), Scale::new(4), [decimal("1.5")])));
}

#[test]
fn scatter_merge_keeps_precision_and_scale() {
	// A merged column must keep the declared type, otherwise a CASE result widens to the default precision.
	for buffer in sample_columns() {
		let ty = buffer.get_type();
		let merged = buffer.scatter_merge(
			&buffer.gather(&[1, 0]),
			&BooleanBuffer::from(vec![true, false]),
			&BooleanBuffer::from(vec![false, true]),
			2,
		);
		assert_eq!(merged.get_type(), ty);
		assert_eq!(merged.get_value(0), buffer.get_value(0), "{ty:?} then row");
		assert_eq!(merged.get_value(1), buffer.get_value(0), "{ty:?} else row");
	}
}

#[test]
fn scatter_merge_of_optional_columns_keeps_precision_and_scale() {
	// The none split must not drop the declared type, otherwise optional merged columns widen.
	for buffer in sample_columns() {
		let ty = buffer.get_type();
		let mut optional = buffer.clone().into_builder();
		optional.push_none();
		let optional = optional.finish();
		let merged = optional.scatter_merge(
			&optional,
			&BooleanBuffer::from(vec![true, false, true]),
			&BooleanBuffer::from(vec![false, true, false]),
			3,
		);
		assert_eq!(merged.get_type(), ValueType::Option(Box::new(ty.clone())));
		assert_eq!(merged.get_value(1), buffer.get_value(1), "{ty:?} defined row");
		assert!(!merged.is_defined(2), "{ty:?} none row");
	}
}

#[test]
fn frame_and_serde_round_trips_keep_precision_and_scale() {
	// A frame or wire round trip must keep the declared type, otherwise a client sees int(76) for int(20).
	for buffer in sample_columns() {
		let ty = buffer.get_type();
		for decoded in round_trips(&buffer) {
			assert_eq!(decoded.get_type(), ty);
			assert_eq!(decoded, buffer, "{ty:?} values");
		}
	}
}

#[test]
fn reset_from_row_keeps_precision_and_scale() {
	// Columns built from a stored row must carry the shape's declared type, not the default precision.
	let types = [
		ValueType::int(Precision::new(20)),
		ValueType::uint(Precision::new(39)),
		ValueType::decimal(Precision::new(12), Scale::new(3)),
	];
	let fields = ["i", "u", "d"]
		.iter()
		.zip(&types)
		.map(|(name, ty)| RowShapeField::new(*name, TypeConstraint::unconstrained(ty.clone())))
		.collect();
	let shape = RowShape::new(RowFamily::Table, fields);
	let mut encoded = shape.allocate_table();
	shape.set_values(
		&mut encoded,
		&[Value::Int(Int::from(-7i64)), Value::Uint(Uint::from(u128::MAX)), Value::Decimal(decimal("1.25"))],
	);
	let row = Row {
		number: RowNumber(1),
		encoded: encoded.freeze().into(),
		shape,
	};
	let mut columns = Columns::new(vec![ColumnWithName::undefined_typed("stale", ValueType::Int4, 1)]);
	columns.reset_from_row(&row);
	assert_eq!(columns.len(), 3);
	for (index, ty) in types.iter().enumerate() {
		assert_eq!(&columns[index].get_type(), ty);
	}
	assert_eq!(columns[0].get_value(0), Value::Int(Int::from(-7i64)));
	assert_eq!(columns[1].get_value(0), Value::Uint(Uint::from(u128::MAX)));
	assert_eq!(columns[2].as_string(0), "1.250");
}

#[test]
fn a_none_survives_when_a_later_value_widens_the_builder() {
	// Widening rebuilds the decimal storage; a none before it must not turn into a zero.
	for wide in ["123.456", "1234567890123456789012345678901234567890.5"] {
		let mut builder = ColumnBuilder::with_capacity(ValueType::decimal(Precision::new(2), Scale::new(1)), 3);
		builder.push_value(Value::Decimal(decimal("1.5")));
		builder.push_value(Value::none());
		builder.push_value(Value::Decimal(decimal(wide)));
		let column = builder.finish();

		assert_eq!(column.len(), 3, "widening to {wide}");
		assert!(column.is_defined(0), "widening to {wide}");
		assert!(!column.is_defined(1), "widening to {wide}");
		assert!(column.is_defined(2), "widening to {wide}");
		assert_eq!(column.get_value(0), Value::Decimal(decimal("1.5")), "widening to {wide}");
		assert!(matches!(column.get_value(1), Value::None { .. }), "widening to {wide}");
		assert_eq!(column.get_value(2), Value::Decimal(decimal(wide)), "widening to {wide}");
	}
}
