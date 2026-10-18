// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::property::{ColumnPropertyKind, ColumnSaturationStrategy},
		evaluate::TargetColumn,
	},
	value::column::factory,
};
use reifydb_evaluate::lower::LoweredExpr;
use reifydb_value::value::{
	column_view::ColumnView,
	constraint::{Constraint, bytes::MaxBytes},
	value_type::ValueType,
};

use crate::{
	common::{Env, Frozen, batch, boolean, cast, cast_constrained, column, none, rows, strings, text},
	compare::{nullable_column, samples},
};

#[test]
fn a_cast_lowers_to_the_field_and_array_of_the_old_path() {
	// Any source and target pair where the field, the values or the error differ is a map column that changes.
	let mut frozen = Frozen::load("a_cast_lowers_to_the_field_and_array_of_the_old_path");
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
				let old = frozen.old(&env, &expression, input.clone());
				assert_eq!(lowered, old, "cast {source:?} to {to:?}");
			}
		}
	}
}

#[test]
fn a_cast_of_a_constant_already_of_the_target_type_is_not_cast() {
	// The old path returns such a constant untouched, so casting it anyway could retype a none.
	let mut frozen = Frozen::inline(&[
		"Field { name: \"none\", data_type: Null, nullable: true } [\"none\"]",
		"Field { name: \"cast(true, type)\", data_type: Boolean } [\"true\", \"true\"]",
		"Field { name: \"cast(none, type)\", data_type: Null, nullable: true } [\"none\", \"none\"]",
	]);
	let env = Env::new();
	let none_type = ColumnView::try_from(&frozen.old(&env, &none(), rows(1)).unwrap()).unwrap().get_type();

	for expression in [cast(boolean("true"), ValueType::Boolean), cast(none(), none_type)] {
		let lowered = env.lowered(&expression, rows(2)).unwrap();
		let old = frozen.old(&env, &expression, rows(2)).unwrap();
		assert_eq!(lowered, old);
	}
}

#[test]
fn a_failed_cast_keeps_the_old_error_code_and_fragment() {
	// The cast error must point at the casted expression and keep its code, otherwise the diagnostic moves.
	let mut frozen = Frozen::inline(&["CAST_002 \"abc\" failed to cast to Int4 []"]);
	let env = Env::new();
	let expression = cast(text("abc"), ValueType::Int4);

	let lowered = env.lowered(&expression, rows(1)).unwrap_err();
	let old = frozen.old(&env, &expression, rows(1)).unwrap_err();

	assert!(lowered.code.starts_with("CAST_"), "{}", lowered.code);
	assert_eq!(lowered, old);
}

#[test]
fn a_cast_under_a_saturating_target_matches_the_old_path() {
	// The target column rides inside the lowered cast, so its none policy must still turn an overflow into none.
	let mut frozen = Frozen::inline(&[
		"Field { name: \"cast(a, type)\", data_type: Int8, nullable: true } [\"1\", \"none\"]",
	]);
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
	let old = frozen.answer(LoweredExpr::new(expression.clone(), "test").evaluate(&ctx));

	assert_eq!(lowered, old);
	assert_eq!(strings(&lowered.unwrap()), vec!["1", "none"]);
}

#[test]
fn a_cast_of_all_none_matches_the_old_path() {
	// An all-none input skips the kernel and answers a typed none, which the declared field must match.
	let mut frozen = Frozen::inline(&[
		"Field { name: \"cast(a, type)\", data_type: LargeUtf8, nullable: true } [\"none\", \"none\", \"none\"]",
		"Field { name: \"cast(a, type)\", data_type: Int64, nullable: true } [\"none\", \"none\", \"none\"]",
		"Field { name: \"cast(a, type)\", data_type: Boolean, nullable: true } [\"none\", \"none\", \"none\"]",
	]);
	let env = Env::new();
	let input = batch(vec![factory::none_typed("a", ValueType::Int4, 3)]);

	for to in [ValueType::Utf8, ValueType::Int8, ValueType::Boolean] {
		let expression = cast(column("a"), to.clone());
		let lowered = env.lowered(&expression, input.clone());
		let old = frozen.old(&env, &expression, input.clone());
		assert!(old.is_ok(), "{to:?}: {old:?}");
		assert_eq!(lowered, old, "{to:?}");
	}
}

#[test]
fn a_cast_over_its_byte_limit_fails_like_the_old_path() {
	// Without the constraint check in the lowered cast, an over-long value passes where the old path fails.
	let mut frozen = Frozen::inline(&[
		"CONSTRAINT_001 \"a\" UTF8 value exceeds maximum byte length: 6 bytes (max: 4 bytes) []",
	]);
	let env = Env::new();
	let expression = cast_constrained(column("a"), ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(4)));
	let input = batch(vec![factory::utf8("a", ["ab", "abcdef"])]);

	let lowered = env.lowered(&expression, input.clone());
	let old = frozen.old(&env, &expression, input);

	assert!(old.is_err(), "{old:?}");
	assert_eq!(lowered, old);
}

#[test]
fn a_constrained_cast_of_a_constant_already_of_the_target_type_still_checks_its_limit() {
	// The same-type constant shortcut must never skip the constraint, otherwise the limit is silently lost.
	let mut frozen = Frozen::inline(&[
		"CONSTRAINT_001 \"abcdef\" UTF8 value exceeds maximum byte length: 6 bytes (max: 4 bytes) []",
	]);
	let env = Env::new();
	let expression = cast_constrained(text("abcdef"), ValueType::Utf8, Constraint::MaxBytes(MaxBytes::new(4)));

	let lowered = env.lowered(&expression, rows(1));
	let old = frozen.old(&env, &expression, rows(1));

	assert!(old.is_err(), "{old:?}");
	assert_eq!(lowered, old);
}
