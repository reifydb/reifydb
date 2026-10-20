// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice;

use arrow_array::RecordBatch;
use reifydb_codec::row::shape::RowShapeField;
use reifydb_core::value::batch::from_encoded_bytes;
use reifydb_sdk::flow::operator::{
	change::BorrowedColumns,
	extern_c::binding::arena::Arena,
	view::{ColumnsView, in_process::InProcessColumnsView},
};
use reifydb_testing_sdk::builders::TestOperatorRowBuilder;
use reifydb_value::value::{
	Value,
	constraint::TypeConstraint,
	dictionary::{DictionaryEntryId, DictionaryId},
	value_type::ValueType,
};

const MINTS: DictionaryId = DictionaryId(7);

fn batch() -> RecordBatch {
	let row = TestOperatorRowBuilder::new(1)
		.with_values(vec![
			Value::DictionaryId(DictionaryEntryId::U4(3)),
			Value::DictionaryId(DictionaryEntryId::U4(4)),
			Value::float8(1.5),
		])
		.with_fields(vec![
			RowShapeField::new("quote", TypeConstraint::dictionary(MINTS, ValueType::Uint4)),
			RowShapeField::unconstrained("loose", ValueType::DictionaryId),
			RowShapeField::unconstrained("price", ValueType::Float8),
		])
		.build();
	from_encoded_bytes(&row.shape, &[row.number], slice::from_ref(&row.encoded)).unwrap()
}

fn answers(view: &impl ColumnsView) -> [Option<DictionaryId>; 4] {
	["quote", "loose", "price", "missing"].map(|name| view.dictionary_of(name).unwrap())
}

#[test]
fn a_dictionary_backed_column_names_its_dictionary_in_process() {
	// Without the dictionary id an operator cannot resolve a text constant to the entry ids it compares against.
	let batch = batch();
	assert_eq!(InProcessColumnsView::new(&batch).dictionary_of("quote").unwrap(), Some(MINTS));
}

#[test]
fn a_plain_or_unbound_or_missing_column_has_no_dictionary_in_process() {
	// A column with no bound dictionary must answer none, never some made up id the host would reject.
	let batch = batch();
	assert_eq!(answers(&InProcessColumnsView::new(&batch))[1..], [None, None, None]);
}

#[test]
fn extern_c_columns_answer_exactly_like_in_process_columns() {
	// A guest loaded over extern-c must see the same dictionary as the in-process harness, otherwise tests lie.
	let batch = batch();
	let mut arena = Arena::new();
	let wire = arena.marshal_columns(&batch).unwrap();
	// SAFETY: `wire` and every buffer it points into live in `arena` and `batch`, both alive for this scope.
	let columns = unsafe { BorrowedColumns::from_extern_c(&wire) };
	assert_eq!(answers(&columns), [Some(MINTS), None, None, None]);
	assert_eq!(answers(&columns), answers(&InProcessColumnsView::new(&batch)));
}
