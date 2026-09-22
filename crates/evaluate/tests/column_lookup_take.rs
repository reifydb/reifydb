// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::identifier::ColumnIdentifier,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_evaluate::expression::{context::EvalContext, eval::evaluate};
use reifydb_rql::expression::{ColumnExpression, Expression};
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		container::dictionary_array::dictionary_array,
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

#[test]
fn a_lookup_limited_by_take_keeps_the_dictionary_of_a_dictionary_column() {
	// Rebuilding the taken rows from values drops the dictionary, so the ids can no longer be decoded downstream.
	let codes = ColumnBuffer::DictionaryId {
		container: dictionary_array([DictionaryEntryId::U4(1), DictionaryEntryId::U4(2)]),
		dictionary_id: Some(DictionaryId(42)),
	};
	let base = EvalContext::testing();
	let mut ctx = base.with_eval(Columns::new(vec![ColumnWithName::new(Fragment::internal("code"), codes)]), 2);
	ctx.take = Some(1);

	let result = evaluate(
		&ctx,
		&Expression::Column(ColumnExpression(ColumnIdentifier::with_alias(
			Fragment::internal("t"),
			Fragment::internal("code"),
		))),
	)
	.unwrap();

	let ColumnBuffer::DictionaryId {
		dictionary_id,
		..
	} = result.data()
	else {
		panic!(
			"a taken dictionary column must stay a plain dictionary column, got {:?}",
			result.data().get_type()
		);
	};
	assert_eq!(
		(result.data().len(), result.data().get_value(0), *dictionary_id),
		(1, Value::DictionaryId(DictionaryEntryId::U4(1)), Some(DictionaryId(42)))
	);
}
