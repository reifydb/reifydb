// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	expression::{Expression, PrefixOperator},
	value::column::factory,
};
use reifydb_value::value::value_type::{ValueType, field::from_field};

use crate::{
	common::{Env, batch, column, frag, none, prefix, rows, strings},
	compare::{nullable_column, samples},
};

fn minus(inner: Expression) -> Expression {
	prefix(PrefixOperator::Minus(frag("-")), inner)
}

#[test]
fn every_signed_and_float_type_negates_like_the_old_path() {
	// Any type whose field, values or error differ is a query that changes under the new path.
	let env = Env::new();
	let signed = |value_type: ValueType| {
		matches!(
			value_type,
			ValueType::Int1
				| ValueType::Int2 | ValueType::Int4
				| ValueType::Int8 | ValueType::Int16
				| ValueType::Float4 | ValueType::Float8
				| ValueType::Decimal { .. }
		)
	};

	for sample in samples().into_iter().filter(|(value, _)| signed(value.get_type())) {
		let input = batch(vec![nullable_column("a", sample.clone())]);
		let lowered = env.lowered(&minus(column("a")), input.clone());
		let old = env.old(&minus(column("a")), input);
		assert!(old.is_ok(), "{:?}: {old:?}", sample.0.get_type());
		assert_eq!(lowered, old, "{:?}", sample.0.get_type());
	}
}

#[test]
fn an_unsigned_column_negates_into_the_signed_type_of_its_width() {
	// The old path answers -uint1 as int1, so a lowered field that keeps the unsigned type changes every consumer.
	let env = Env::new();
	let widths = [
		(ValueType::Uint1, ValueType::Int1),
		(ValueType::Uint2, ValueType::Int2),
		(ValueType::Uint4, ValueType::Int4),
		(ValueType::Uint8, ValueType::Int8),
		(ValueType::Uint16, ValueType::Int16),
	];

	for (unsigned, signed) in widths {
		let sample = samples().into_iter().find(|(value, _)| value.get_type() == unsigned).unwrap();
		let input = batch(vec![nullable_column("a", sample)]);
		let lowered = env.lowered(&minus(column("a")), input.clone()).unwrap();
		let old = env.old(&minus(column("a")), input).unwrap();
		assert_eq!(lowered, old, "{unsigned:?}");
		let declared =
			from_field(&lowered.0).unwrap().value_type.map(|value_type| value_type.inner_type().clone());
		assert_eq!(declared, Some(signed), "{unsigned:?}");
	}
}

#[test]
fn negating_the_smallest_signed_value_errors_with_the_prefix_fragment() {
	// Negation must never wrap, otherwise -int1::MIN silently stays negative.
	let env = Env::new();
	let input = batch(vec![factory::int1("a", [i8::MIN])]);

	let lowered = env.lowered(&minus(column("a")), input.clone());
	let old = env.old(&minus(column("a")), input);

	assert!(old.is_err(), "{old:?}");
	assert_eq!(lowered, old);
}

#[test]
fn an_unsigned_value_beyond_the_signed_range_errors_like_the_old_path() {
	// A uint1 of 200 has no int1 negation, so it must error rather than wrap into the signed range.
	let env = Env::new();
	let input = batch(vec![factory::uint1("a", [200u8])]);

	let lowered = env.lowered(&minus(column("a")), input.clone());
	let old = env.old(&minus(column("a")), input);

	assert!(old.is_err(), "{old:?}");
	assert_eq!(lowered, old);
}

#[test]
fn none_rows_stay_none_and_an_untyped_none_passes_through() {
	// A none must stay none per row, and a bare none must not turn into a typed zero.
	let env = Env::new();
	let input = batch(vec![factory::int4_optional("a", [Some(1), None, Some(-3)])]);

	let lowered = env.lowered(&minus(column("a")), input.clone()).unwrap();
	let old = env.old(&minus(column("a")), input).unwrap();
	assert_eq!(lowered, old);
	assert_eq!(strings(&lowered), vec!["-1", "none", "3"]);

	let lowered = env.lowered(&minus(none()), rows(2));
	let old = env.old(&minus(none()), rows(2));
	assert_eq!(lowered, old);
}

#[test]
fn a_text_operand_errors_like_the_old_path() {
	// Text has no negation, so the error must match the old path instead of a DataFusion type error.
	let env = Env::new();
	let input = batch(vec![factory::utf8("a", ["x"])]);

	let lowered = env.lowered(&minus(column("a")), input.clone());
	let old = env.old(&minus(column("a")), input);

	assert!(old.is_err(), "{old:?}");
	assert_eq!(lowered, old);
}
