// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::property::{ColumnPropertyKind, ColumnSaturationStrategy},
		evaluate::TargetColumn,
	},
	value::column::factory,
};
use reifydb_evaluate::{
	expression::{compile::compile_expression, context::CompileContext},
	lower::LoweredExpr,
};
use reifydb_value::value::{column_view::ColumnView, value_type::ValueType};

use crate::{
	common::{Env, batch, boolean, cast, column, none, rows, strings, text},
	compare::{nullable_column, samples},
};

#[test]
fn a_cast_lowers_to_the_field_and_array_of_the_old_path() {
	// Any source and target pair where the field, the values or the error differ is a map column that changes.
	let env = Env::new();
	let targets: Vec<ValueType> = samples().into_iter().map(|(first, _)| first.get_type()).collect();

	for (first, second) in samples() {
		let source = first.get_type();
		let plain = factory::from_many("a", first.clone(), 2);
		let nullable = nullable_column("a", (first, second));
		for input in [batch(vec![plain]), batch(vec![nullable])] {
			for to in &targets {
				let expression = cast(column("a"), to.clone());
				let lowered = env.lowered(&expression, input.clone());
				let old = env.old(&expression, input.clone());
				assert_eq!(lowered, old, "cast {source:?} to {to:?}");
			}
		}
	}
}

#[test]
fn a_cast_of_a_constant_already_of_the_target_type_is_not_cast() {
	// The old path returns such a constant untouched, so casting it anyway could retype a none.
	let env = Env::new();
	let none_type = ColumnView::try_from(&env.old(&none(), rows(1)).unwrap()).unwrap().get_type();

	for expression in [cast(boolean("true"), ValueType::Boolean), cast(none(), none_type)] {
		let lowered = env.lowered(&expression, rows(2)).unwrap();
		let old = env.old(&expression, rows(2)).unwrap();
		assert_eq!(lowered, old);
	}
}

#[test]
fn a_failed_cast_keeps_the_old_error_code_and_fragment() {
	// The cast error must point at the casted expression and keep its code, otherwise the diagnostic moves.
	let env = Env::new();
	let expression = cast(text("abc"), ValueType::Int4);

	let lowered = env.lowered(&expression, rows(1)).unwrap_err();
	let old = env.old(&expression, rows(1)).unwrap_err();

	assert!(lowered.code.starts_with("CAST_"), "{}", lowered.code);
	assert_eq!(lowered, old);
}

#[test]
fn a_cast_under_a_saturating_target_matches_the_old_path() {
	// The target column rides inside the lowered cast, so its none policy must still turn an overflow into none.
	let env = Env::new();
	let expression = cast(column("a"), ValueType::Int1);
	let target = TargetColumn::Partial {
		source_name: None,
		column_name: None,
		column_type: ValueType::Int1,
		properties: vec![ColumnPropertyKind::Saturation(ColumnSaturationStrategy::None)],
	};
	let mut ctx = env.ctx(batch(vec![factory::int4("a", [1, 300])]));
	ctx.target = Some(target);

	let lowered = LoweredExpr::new(expression.clone(), "test").evaluate(&ctx);
	let old = compile_expression(
		&CompileContext {
			symbols: &env.symbols,
		},
		&expression,
	)
	.unwrap()
	.execute(&ctx);

	assert_eq!(lowered, old);
	assert_eq!(strings(&lowered.unwrap()), vec!["1", "none"]);
}

#[test]
fn a_cast_of_all_none_matches_the_old_path() {
	// An all-none input skips the kernel and answers a typed none, which the declared field must match.
	let env = Env::new();
	let input = batch(vec![factory::none_typed("a", ValueType::Int4, 3)]);

	for to in [ValueType::Utf8, ValueType::Int8, ValueType::Boolean] {
		let expression = cast(column("a"), to.clone());
		let lowered = env.lowered(&expression, input.clone());
		let old = env.old(&expression, input.clone());
		assert!(old.is_ok(), "{to:?}: {old:?}");
		assert_eq!(lowered, old, "{to:?}");
	}
}
