// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::BooleanBuffer;
use arrow_schema::FieldRef;
use reifydb_codec::row::shape::{RowFamily, RowShape, RowShapeField};
use reifydb_core::{
	row::Row,
	value::{
		batch::{batch, from_row, take_rows},
		column::{builder::ColumnBuilder, factory, scatter::scatter_merge},
	},
};
use reifydb_value::value::{
	Value,
	column_view::{ColumnView, ViewData},
	constraint::{TypeConstraint, precision::Precision, scale::Scale},
	container::decimal_array::DecimalView,
	decimal::Decimal,
	row_number::RowNumber,
	system_columns::is_system_field,
	value_type::ValueType,
};

fn view(column: &(FieldRef, ArrayRef)) -> ColumnView<'_> {
	ColumnView::try_from(column).unwrap()
}

fn take_column(column: &(FieldRef, ArrayRef), indices: &[usize]) -> (FieldRef, ArrayRef) {
	let rows = take_rows(&batch(vec![column.clone()]).unwrap(), indices).unwrap();
	(rows.schema_ref().fields()[0].clone(), rows.column(0).clone())
}

fn user_column(rows: &RecordBatch, index: usize) -> ColumnView<'_> {
	let schema = rows.schema_ref();
	let position = (0..rows.num_columns()).filter(|&i| !is_system_field(schema.field(i))).nth(index).unwrap();
	ColumnView::try_from((rows.column(position), schema.field(position))).unwrap()
}

fn user_column_count(rows: &RecordBatch) -> usize {
	rows.schema_ref().fields().iter().filter(|field| !is_system_field(field)).count()
}

fn is_decimal256(column: &(FieldRef, ArrayRef)) -> bool {
	let column = view(column);
	match &column.data {
		ViewData::Decimal(DecimalView::Decimal128(_)) => false,
		ViewData::Decimal(DecimalView::Decimal256(_)) => true,
		_ => panic!("expected an int, uint or decimal column, got {:?}", column.get_type()),
	}
}

fn decimal(text: &str) -> Decimal {
	text.parse().unwrap()
}

fn family_types(precision: u8) -> [ValueType; 1] {
	let precision = Precision::new(precision);
	[ValueType::decimal(precision, Scale::new(4))]
}

fn sample_columns() -> [(FieldRef, ArrayRef); 1] {
	[factory::decimal("c", Precision::new(12), Scale::new(3), [decimal("1.25"), decimal("-0.5")])]
}

fn sample_columns_with_a_none() -> [(FieldRef, ArrayRef); 1] {
	[factory::decimal_with_bitvec(
		"c",
		Precision::new(12),
		Scale::new(3),
		[decimal("1.25"), decimal("2.5"), decimal("-0.5")],
		vec![true, false, true],
	)]
}

#[test]
fn precision_38_is_decimal128_for_every_family_type() {
	// Precision 38 must stay on the 16 byte layout, otherwise every narrow column doubles its memory.
	for ty in family_types(38) {
		assert!(!is_decimal256(&ColumnBuilder::with_capacity(ty.clone(), 1).finish("c")), "{ty:?} builder");
		assert!(!is_decimal256(&factory::none_typed("c", ty.clone(), 2)), "{ty:?} none typed");
		assert_eq!(view(&factory::none_typed("c", ty.clone(), 2)).get_type(), ValueType::Option(Box::new(ty)));
	}
	assert!(!is_decimal256(&factory::decimal("c", Precision::new(38), Scale::new(4), [decimal("1.5")])));
}

#[test]
fn precision_39_is_decimal256_for_every_family_type() {
	// Precision 39 needs more than 128 bits, otherwise a full width value overflows the native storage.
	for ty in family_types(39) {
		assert!(is_decimal256(&ColumnBuilder::with_capacity(ty.clone(), 1).finish("c")), "{ty:?} builder");
		assert!(is_decimal256(&factory::none_typed("c", ty.clone(), 2)), "{ty:?} none typed");
		assert_eq!(view(&factory::none_typed("c", ty.clone(), 2)).get_type(), ValueType::Option(Box::new(ty)));
	}
	assert!(is_decimal256(&factory::decimal("c", Precision::new(39), Scale::new(4), [decimal("1.5")])));
}

#[test]
fn scatter_merge_keeps_precision_and_scale() {
	// A merged column must keep the declared type, otherwise a CASE result widens to the default precision.
	for column in sample_columns() {
		let buffer = view(&column);
		let ty = buffer.get_type();
		let gathered = take_column(&column, &[1, 0]);
		let merged = scatter_merge(
			&buffer,
			&view(&gathered),
			&BooleanBuffer::from(vec![true, false]),
			&BooleanBuffer::from(vec![false, true]),
			2,
			"c",
		)
		.unwrap();
		let merged = view(&merged);
		assert_eq!(merged.get_type(), ty);
		assert_eq!(merged.get_value(0), buffer.get_value(0), "{ty:?} then row");
		assert_eq!(merged.get_value(1), buffer.get_value(0), "{ty:?} else row");
	}
}

#[test]
fn scatter_merge_of_optional_columns_keeps_precision_and_scale() {
	// The none split must not drop the declared type, otherwise optional merged columns widen.
	for column in sample_columns() {
		let buffer = view(&column);
		let ty = buffer.get_type();
		let mut optional = ColumnBuilder::from_view(&buffer);
		optional.push_none();
		let optional = optional.finish("c");
		let merged = scatter_merge(
			&view(&optional),
			&view(&optional),
			&BooleanBuffer::from(vec![true, false, true]),
			&BooleanBuffer::from(vec![false, true, false]),
			3,
			"c",
		)
		.unwrap();
		let merged = view(&merged);
		assert_eq!(merged.get_type(), ValueType::Option(Box::new(ty.clone())));
		assert_eq!(merged.get_value(1), buffer.get_value(1), "{ty:?} defined row");
		assert!(!merged.is_defined(2), "{ty:?} none row");
	}
}

#[test]
fn reset_from_row_keeps_precision_and_scale() {
	// Columns built from a stored row must carry the shape's declared type, not the default precision.
	let types = [ValueType::decimal(Precision::new(12), Scale::new(3))];
	let fields = ["d"]
		.iter()
		.zip(&types)
		.map(|(name, ty)| RowShapeField::new(*name, TypeConstraint::unconstrained(ty.clone())))
		.collect();
	let shape = RowShape::new(RowFamily::Table, fields);
	let mut encoded = shape.allocate_table();
	shape.set_values(&mut encoded, &[Value::Decimal(decimal("1.25"))]);
	let row = Row {
		number: RowNumber(1),
		encoded: encoded.freeze().into(),
		shape,
	};
	let columns = from_row(&row).unwrap();
	assert_eq!(user_column_count(&columns), 1);
	for (index, ty) in types.iter().enumerate() {
		assert_eq!(&user_column(&columns, index).get_type(), ty);
	}
	assert_eq!(user_column(&columns, 0).as_string(0), "1.250");
}

#[test]
fn a_none_survives_when_a_later_value_widens_the_builder() {
	// Widening rebuilds the decimal storage; a none before it must not turn into a zero.
	for wide in ["123.456", "1234567890123456789012345678901234567890.5"] {
		let mut builder = ColumnBuilder::with_capacity(ValueType::decimal(Precision::new(2), Scale::new(1)), 3);
		builder.push_value(Value::Decimal(decimal("1.5")));
		builder.push_value(Value::none());
		builder.push_value(Value::Decimal(decimal(wide)));
		let column = builder.finish("c");
		let column = view(&column);

		assert_eq!(column.len(), 3, "widening to {wide}");
		assert!(column.is_defined(0), "widening to {wide}");
		assert!(!column.is_defined(1), "widening to {wide}");
		assert!(column.is_defined(2), "widening to {wide}");
		assert_eq!(column.get_value(0), Value::Decimal(decimal("1.5")), "widening to {wide}");
		assert!(matches!(column.get_value(1), Value::None { .. }), "widening to {wide}");
		assert_eq!(column.get_value(2), Value::Decimal(decimal(wide)), "widening to {wide}");
	}
}

#[test]
fn a_none_in_a_family_column_survives_reorder() {
	// Reorder must move the null bit with its value, otherwise a shuffled none becomes a stale number.
	for original in sample_columns_with_a_none() {
		let indices = [2, 0, 1];
		let column = take_column(&original, &indices);
		let original = view(&original);
		let column = view(&column);
		let ty = original.get_type();
		assert_eq!(column.len(), 3, "{ty:?}");
		for (new_index, &old_index) in indices.iter().enumerate() {
			assert_eq!(
				column.is_defined(new_index),
				original.is_defined(old_index),
				"{ty:?} row {new_index}"
			);
			if original.is_defined(old_index) {
				assert_eq!(
					column.get_value(new_index),
					original.get_value(old_index),
					"{ty:?} row {new_index}"
				);
			}
		}
	}
}

#[test]
fn a_none_in_a_family_column_survives_gather_with_a_repeated_index() {
	// A repeated index must read the null bit on every read, otherwise a duplicated none returns a stale value.
	for original in sample_columns_with_a_none() {
		let indices = [1, 1, 2, 0];
		let gathered = take_column(&original, &indices);
		let original = view(&original);
		let gathered = view(&gathered);
		let ty = original.get_type();
		assert_eq!(gathered.len(), indices.len(), "{ty:?}");
		for (new_index, &old_index) in indices.iter().enumerate() {
			assert_eq!(
				gathered.is_defined(new_index),
				original.is_defined(old_index),
				"{ty:?} row {new_index}"
			);
			if original.is_defined(old_index) {
				assert_eq!(
					gathered.get_value(new_index),
					original.get_value(old_index),
					"{ty:?} row {new_index}"
				);
			}
		}
	}
}
