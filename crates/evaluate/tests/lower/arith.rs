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
use reifydb_value::value::value_type::{ValueType, field::from_field};

use crate::{
	common::{ARITH_OPS, Env, arith, batch, boolean, column, none, number, rows, strings, text},
	compare::{nullable_column, samples},
};

#[test]
fn every_type_pair_computes_like_the_old_path() {
	// Any pair where the field, the values or the error differ is a filter that changes under the new path.
	let env = Env::new();

	for left in samples() {
		for right in samples() {
			let input =
				batch(vec![nullable_column("l", left.clone()), nullable_column("r", right.clone())]);
			for op in ARITH_OPS {
				let expression = arith(op, column("l"), column("r"));
				let lowered = env.lowered(&expression, input.clone());
				let old = env.old(&expression, input.clone());
				assert_eq!(lowered, old, "{:?} {op} {:?}", left.0.get_type(), right.0.get_type());
			}
		}
	}
}

#[test]
fn int4_plus_int4_declares_an_int8_field() {
	// The declared field is what every node above reads, so it must be the promoted type, not the left type.
	let env = Env::new();
	let input = batch(vec![factory::int4("l", [1]), factory::int4("r", [2])]);

	let lowered = env.lowered(&arith("+", column("l"), column("r")), input).unwrap();

	assert_eq!(from_field(&lowered.0).unwrap().value_type, Some(ValueType::Int8));
}

#[test]
fn an_overflow_errors_with_the_same_span_as_the_old_path() {
	// The range error must point at the whole expression, not the operator token alone.
	let env = Env::new();
	let input = batch(vec![factory::int16("l", [i128::MAX]), factory::int16("r", [1])]);
	let expression = arith("+", column("l"), column("r"));

	let lowered = env.lowered(&expression, input.clone()).unwrap_err();
	let old = env.old(&expression, input).unwrap_err();

	assert_eq!(lowered.code, "NUMBER_002");
	assert_eq!(lowered, old);
}

#[test]
fn a_division_by_zero_errors_like_the_old_path() {
	// Division by zero must stay an error, never a none or an infinity.
	let env = Env::new();
	let input = batch(vec![factory::int4("l", [1]), factory::int4("r", [0])]);

	for op in ["/", "%"] {
		let expression = arith(op, column("l"), column("r"));
		let lowered = env.lowered(&expression, input.clone()).unwrap_err();
		let old = env.old(&expression, input.clone()).unwrap_err();
		assert_eq!(lowered.code, "NUMBER_007", "{op}");
		assert_eq!(lowered, old, "{op}");
	}
}

#[test]
fn the_none_saturation_policy_of_the_target_gives_none_on_overflow() {
	// The target column rides inside the lowered call, so its policy must still turn an overflow into none.
	let env = Env::new();
	let input = batch(vec![factory::int16("l", [i128::MAX]), factory::int16("r", [1])]);
	let expression = arith("+", column("l"), column("r"));
	let target = TargetColumn::Partial {
		source_name: None,
		column_name: None,
		column_type: ValueType::Int16,
		properties: vec![ColumnPropertyKind::Saturation(ColumnSaturationStrategy::None)],
	};
	let mut ctx = env.ctx(input);
	ctx.target = Some(target);

	let lowered = LoweredExpr::new(expression.clone(), "test").evaluate(&ctx).unwrap();
	let compile_ctx = CompileContext {
		symbols: &env.symbols,
	};
	let old = compile_expression(&compile_ctx, &expression).unwrap().execute(&ctx).unwrap();

	assert_eq!(strings(&lowered), vec!["none"]);
	assert_eq!(lowered, old);
}

#[test]
fn text_plus_a_number_concatenates_like_the_old_path() {
	// Plus on text is ours, not DataFusion's, so it must keep concatenating.
	let env = Env::new();

	for expression in [arith("+", text("a"), number("1")), arith("+", number("1"), text("a"))] {
		let lowered = env.lowered(&expression, rows(1)).unwrap();
		assert_eq!(lowered, env.old(&expression, rows(1)).unwrap(), "{expression:?}");
	}
}

#[test]
fn an_all_none_operand_keeps_the_declared_field() {
	// The kernel types an all none batch from empty input, which must agree with the declared field.
	let env = Env::new();
	let input = batch(vec![factory::none_typed("l", ValueType::Int4, 2), factory::int4("r", [1, 2])]);

	for op in ARITH_OPS {
		let expression = arith(op, column("l"), column("r"));
		let lowered = env.lowered(&expression, input.clone()).unwrap();
		assert_eq!(lowered, env.old(&expression, input.clone()).unwrap(), "{op}");
	}
}

#[test]
fn an_untyped_none_operand_takes_the_other_sides_type() {
	// none plus a column answers none typed as that column, so the declared field must follow the same rule.
	let env = Env::new();
	let input = batch(vec![factory::int4("r", [1, 2])]);

	for op in ARITH_OPS {
		for expression in
			[arith(op, none(), column("r")), arith(op, column("r"), none()), arith(op, none(), none())]
		{
			let lowered = env.lowered(&expression, input.clone()).unwrap();
			assert_eq!(lowered, env.old(&expression, input.clone()).unwrap(), "{expression:?}");
		}
	}
}

#[test]
fn a_bool_operand_errors_like_the_old_path() {
	// The kernel owns the type error, so it must come out with the same code and span as today.
	let env = Env::new();

	for op in ARITH_OPS {
		let expression = arith(op, boolean("true"), number("1"));
		let lowered = env.lowered(&expression, rows(1)).unwrap_err();
		assert_eq!(lowered, env.old(&expression, rows(1)).unwrap_err(), "{op}");
	}
}
