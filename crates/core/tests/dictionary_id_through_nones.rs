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
	constraint::TypeConstraint,
	container::dictionary_array::dictionary_array,
	dictionary::{DictionaryEntryId, DictionaryId},
	row_number::RowNumber,
	value_type::ValueType,
};

fn tagged(entries: &[DictionaryEntryId], id: u64) -> ColumnBuffer {
	ColumnBuffer::DictionaryId {
		container: dictionary_array(entries.iter().copied()),
		dictionary_id: Some(DictionaryId(id)),
	}
}

fn dictionary_id_of(buffer: &ColumnBuffer) -> Option<DictionaryId> {
	match buffer {
		ColumnBuffer::DictionaryId {
			dictionary_id,
			..
		} => *dictionary_id,
		other => panic!("expected a dictionary id column, got {:?}", other.get_type()),
	}
}

fn dictionary_shape(id: u64) -> RowShape {
	let constraint = TypeConstraint::dictionary(DictionaryId(id), ValueType::Uint4);
	RowShape::new(RowFamily::Table, vec![RowShapeField::new("tag", constraint)])
}

#[test]
fn appending_a_row_to_an_all_none_dictionary_column_sets_the_shapes_dictionary_id() {
	// A column retyped from all none must still carry the dictionary id, or its ids can never be decoded.
	let shape = dictionary_shape(2);
	let mut columns = Columns::new(vec![ColumnWithName::undefined_typed("tag", ValueType::DictionaryId, 2)]);
	let mut row = shape.allocate_table();
	shape.set_values(&mut row, &[Value::DictionaryId(DictionaryEntryId::U4(5))]);
	columns.append_rows(&shape, [row.freeze()], vec![]).unwrap();
	assert_eq!(columns[0].get_value(2), Value::DictionaryId(DictionaryEntryId::U4(5)));
	assert_eq!(dictionary_id_of(&columns[0]), Some(DictionaryId(2)));
}

#[test]
fn set_dictionary_id_on_an_option_builder_reaches_the_inner_dictionary_column() {
	// An Option(DictionaryId) builder must pass the id to its inner column instead of dropping it.
	let mut builder = ColumnBuilder::with_capacity(ValueType::Option(Box::new(ValueType::DictionaryId)), 2);
	builder.set_dictionary_id(DictionaryId(7));
	builder.push_none();
	assert_eq!(dictionary_id_of(&builder.finish()), Some(DictionaryId(7)));
}

#[test]
fn a_row_holding_none_in_a_dictionary_field_keeps_the_dictionary_id() {
	// A dictionary column whose first row is none must still carry the shape's dictionary id.
	let shape = dictionary_shape(3);
	let mut encoded = shape.allocate_table();
	shape.set_values(&mut encoded, &[Value::none()]);
	let row = Row {
		number: RowNumber(1),
		encoded: encoded.freeze().into(),
		shape,
	};
	let columns = Columns::from_row(&row);
	assert!(!columns[0].is_defined(0));
	assert_eq!(dictionary_id_of(&columns[0]), Some(DictionaryId(3)));
}

#[test]
fn pushing_an_id_after_only_nones_keeps_the_builders_dictionary_id() {
	// Retyping the inner column on the first defined row must not throw away the dictionary id it carries.
	let mut builder = tagged(&[], 42).into_builder();
	builder.push_none();
	builder.push_none();
	builder.push_value(Value::DictionaryId(DictionaryEntryId::U4(9)));
	let finished = builder.finish();
	assert_eq!(finished.get_value(2), Value::DictionaryId(DictionaryEntryId::U4(9)));
	assert_eq!(dictionary_id_of(&finished), Some(DictionaryId(42)));
}

#[test]
fn scatter_merge_of_two_columns_on_the_same_dictionary_keeps_the_dictionary_id() {
	// A merged branch result must still name its dictionary, or the ids it holds become undecodable.
	let then_column = tagged(&[DictionaryEntryId::U4(1), DictionaryEntryId::U4(2)], 7);
	let else_column = tagged(&[DictionaryEntryId::U4(3), DictionaryEntryId::U4(4)], 7);
	let then_mask = BooleanBuffer::from(vec![true, false]);
	let else_mask = BooleanBuffer::from(vec![false, true]);
	let merged = then_column.scatter_merge(&else_column, &then_mask, &else_mask, 2);
	assert_eq!(merged.get_value(0), Value::DictionaryId(DictionaryEntryId::U4(1)));
	assert_eq!(merged.get_value(1), Value::DictionaryId(DictionaryEntryId::U4(4)));
	assert_eq!(dictionary_id_of(&merged), Some(DictionaryId(7)));
}
