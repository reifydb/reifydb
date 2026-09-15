// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{
	bytes::EncodedBytes,
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_value::{
	Result,
	value::{Value, digest::Digest, row_number::RowNumber, value_type::ValueType},
};

const ACCURACY: u32 = 10_000;

fn digest_type(inner: ValueType, accuracy: u32) -> ValueType {
	ValueType::Digest {
		inner: Box::new(inner),
		accuracy,
	}
}

fn digest_value(inner: ValueType, accuracy: u32, values: &[i32]) -> Value {
	let mut digest = Digest::new(inner, accuracy).unwrap();
	for value in values {
		digest.add_value(&Value::Int4(*value)).unwrap();
	}
	Value::Digest(Box::new(digest))
}

fn shape_of(fields: &[(&str, ValueType)]) -> RowShape {
	RowShape::new(
		RowFamily::Table,
		fields.iter().map(|(name, ty)| RowShapeField::unconstrained(*name, ty.clone())).collect(),
	)
}

fn encode_rows(shape: &RowShape, rows: &[Vec<Value>]) -> Vec<EncodedBytes> {
	rows.iter()
		.map(|values| {
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, values);
			EncodedBytes::from(row.freeze())
		})
		.collect()
}

fn empty_columns(shape: &RowShape) -> Columns {
	Columns::new(
		shape.fields()
			.iter()
			.map(|field| {
				ColumnWithName::new(
					field.name.as_str(),
					ColumnBuffer::with_capacity(field.constraint.get_type(), 0),
				)
			})
			.collect(),
	)
}

fn append(shape: &RowShape, columns: &mut Columns, rows: &[Vec<Value>]) -> Result<()> {
	let row_numbers = (1..=rows.len() as u64).map(RowNumber).collect();
	columns.append_rows(shape, encode_rows(shape, rows), row_numbers)
}

fn cells(column: &ColumnBuffer) -> Vec<Value> {
	(0..column.len()).map(|row| column.get_value(row)).collect()
}

#[test]
fn all_defined_rows_decode_into_a_digest_column_with_the_field_inner_type_and_accuracy() {
	// A digest column typed from anything but the field would merge digests of another accuracy.
	let ty = digest_type(ValueType::Int4, ACCURACY);
	let shape = shape_of(&[("k", ValueType::Int4), ("d", ty.clone())]);
	let rows = vec![
		vec![Value::Int4(1), digest_value(ValueType::Int4, ACCURACY, &[5, 10, 15])],
		vec![Value::Int4(2), digest_value(ValueType::Int4, ACCURACY, &[])],
	];
	let mut columns = empty_columns(&shape);
	append(&shape, &mut columns, &rows).unwrap();

	let column = &columns[1];
	assert!(
		matches!(
			column,
			ColumnBuffer::Digest { inner, accuracy, .. } if *inner == ValueType::Int4 && *accuracy == ACCURACY
		),
		"expected a plain digest column, got {:?}",
		column.get_type()
	);
	assert_eq!(cells(column), vec![rows[0][1].clone(), rows[1][1].clone()]);
}

#[test]
fn a_none_digest_row_decodes_into_an_option_over_a_digest_column() {
	// The fallback path must keep the digest type under the option, or a later digest push panics.
	let ty = digest_type(ValueType::Int4, 50_000);
	let present = digest_value(ValueType::Int4, 50_000, &[-3, 8]);
	let rows = vec![
		vec![present.clone(), Value::Int4(1)],
		vec![Value::none_of(ty.clone()), Value::Int4(2)],
		vec![present.clone(), Value::none_of(ValueType::Int4)],
	];
	for option_field in [false, true] {
		let field_type = if option_field {
			ValueType::Option(Box::new(ty.clone()))
		} else {
			ty.clone()
		};
		let shape = shape_of(&[("d", field_type), ("k", ValueType::Int4)]);
		let mut columns = empty_columns(&shape);
		append(&shape, &mut columns, &rows).unwrap();

		assert_eq!(
			columns[0].get_type(),
			ValueType::Option(Box::new(ty.clone())),
			"option field {option_field}"
		);
		let values = cells(&columns[0]);
		assert_eq!(values[0], present);
		assert!(matches!(values[1], Value::None { .. }));
		assert_eq!(values[2], present);
	}
}

#[test]
fn a_digest_column_of_another_accuracy_is_a_type_mismatch_naming_the_column() {
	// Appending into a column of another accuracy would put incompatible digests in one column.
	let shape = shape_of(&[("lat", digest_type(ValueType::Int4, ACCURACY))]);
	let mut columns = Columns::new(vec![ColumnWithName::new(
		"lat",
		ColumnBuffer::with_capacity(digest_type(ValueType::Int4, 20_000), 0),
	)]);
	let err = append(&shape, &mut columns, &[vec![digest_value(ValueType::Int4, ACCURACY, &[1])]]).unwrap_err();
	assert!(err.to_string().contains("'lat'"), "{err}");
}

#[test]
fn a_scan_decode_of_digest_rows_keeps_the_digest_type_under_the_option() {
	// A scan that decoded a digest column as another type would lose the accuracy the merge relies on.
	let ty = digest_type(ValueType::Int4, ACCURACY);
	let shape = shape_of(&[("d", ty.clone())]);
	let rows = vec![vec![digest_value(ValueType::Int4, ACCURACY, &[1, 2, 3])], vec![Value::none_of(ty.clone())]];
	let encoded = encode_rows(&shape, &rows);

	let scanned = Columns::from_encoded_bytes(&shape, &[RowNumber(1), RowNumber(2)], &encoded);
	assert_eq!(scanned[0].get_type(), ValueType::Option(Box::new(ty.clone())));
	assert_eq!(scanned[0].get_value(0), rows[0][0]);
	assert!(matches!(scanned[0].get_value(1), Value::None { .. }));
}
