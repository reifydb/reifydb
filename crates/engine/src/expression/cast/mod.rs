// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod any;
pub mod blob;
pub mod boolean;
pub mod number;
pub mod temporal;
pub mod text;
pub mod uuid;

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::{err, error::diagnostic::cast, fragment::LazyFragment, value::r#type::Type};

use crate::expression::context::EvalContext;

pub fn cast_column_data(
	ctx: &EvalContext,
	data: &ColumnData,
	target: Type,
	lazy_fragment: impl LazyFragment + Clone,
) -> crate::Result<ColumnData> {
	if let ColumnData::Undefined(container) = data {
		let mut result = ColumnData::with_capacity(target.clone(), container.len());
		for _ in 0..container.len() {
			result.push_undefined();
		}
		return Ok(result);
	}
	// Handle Option-wrapped data: cast the inner data, then re-wrap with the bitvec
	if let ColumnData::Option {
		inner,
		bitvec,
	} = data
	{
		let inner_target = match &target {
			Type::Option(t) => t.as_ref().clone(),
			other => other.clone(),
		};
		let cast_inner = cast_column_data(ctx, inner, inner_target, lazy_fragment)?;
		return Ok(ColumnData::Option {
			inner: Box::new(cast_inner),
			bitvec: bitvec.clone(),
		});
	}
	let source_type = data.get_type();
	if target == source_type {
		return Ok(data.clone());
	}
	match (&source_type, &target) {
		(Type::Any, _) => any::from_any(ctx, data, target, lazy_fragment),
		(_, t) if t.is_number() => number::to_number(ctx, data, target, lazy_fragment),
		(_, t) if t.is_blob() => blob::to_blob(data, lazy_fragment),
		(_, t) if t.is_bool() => boolean::to_boolean(data, lazy_fragment),
		(_, t) if t.is_utf8() => text::to_text(data, lazy_fragment),
		(_, t) if t.is_temporal() => temporal::to_temporal(data, target, lazy_fragment),
		(_, t) if t.is_uuid() => uuid::to_uuid(data, target, lazy_fragment),
		(source, t) if source.is_uuid() || t.is_uuid() => uuid::to_uuid(data, target, lazy_fragment),
		_ => {
			err!(cast::unsupported_cast(lazy_fragment.fragment(), source_type, target))
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::value::column::data::ColumnData;
	use reifydb_function::registry::Functions;
	use reifydb_rql::expression::{
		CastExpression, ConstantExpression,
		ConstantExpression::Number,
		Expression::{Cast, Constant, Prefix},
		PrefixExpression, PrefixOperator, TypeExpression,
	};
	use reifydb_runtime::clock::Clock;
	use reifydb_type::{fragment::Fragment, value::r#type::Type};

	use crate::expression::{context::EvalContext, eval::evaluate};

	#[test]
	fn test_cast_integer() {
		let mut ctx = EvalContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::testing_empty(),
				expression: Box::new(Constant(Number {
					fragment: Fragment::internal("42"),
				})),
				to: TypeExpression {
					fragment: Fragment::testing_empty(),
					ty: Type::Int4,
				},
			}),
			&Functions::empty(),
			&Clock::default(),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::int4([42]));
	}

	#[test]
	fn test_cast_negative_integer() {
		let mut ctx = EvalContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::testing_empty(),
				expression: Box::new(Prefix(PrefixExpression {
					operator: PrefixOperator::Minus(Fragment::testing_empty()),
					expression: Box::new(Constant(Number {
						fragment: Fragment::internal("42"),
					})),
					fragment: Fragment::testing_empty(),
				})),
				to: TypeExpression {
					fragment: Fragment::testing_empty(),
					ty: Type::Int4,
				},
			}),
			&Functions::empty(),
			&Clock::default(),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::int4([-42]));
	}

	#[test]
	fn test_cast_negative_min() {
		let mut ctx = EvalContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::testing_empty(),
				expression: Box::new(Prefix(PrefixExpression {
					operator: PrefixOperator::Minus(Fragment::testing_empty()),
					expression: Box::new(Constant(Number {
						fragment: Fragment::internal("128"),
					})),
					fragment: Fragment::testing_empty(),
				})),
				to: TypeExpression {
					fragment: Fragment::testing_empty(),
					ty: Type::Int1,
				},
			}),
			&Functions::empty(),
			&Clock::default(),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::int1([-128]));
	}

	#[test]
	fn test_cast_float_8() {
		let mut ctx = EvalContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::testing_empty(),
				expression: Box::new(Constant(Number {
					fragment: Fragment::internal("4.2"),
				})),
				to: TypeExpression {
					fragment: Fragment::testing_empty(),
					ty: Type::Float8,
				},
			}),
			&Functions::empty(),
			&Clock::default(),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::float8([4.2]));
	}

	#[test]
	fn test_cast_float_4() {
		let mut ctx = EvalContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::testing_empty(),
				expression: Box::new(Constant(Number {
					fragment: Fragment::internal("4.2"),
				})),
				to: TypeExpression {
					fragment: Fragment::testing_empty(),
					ty: Type::Float4,
				},
			}),
			&Functions::empty(),
			&Clock::default(),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::float4([4.2]));
	}

	#[test]
	fn test_cast_negative_float_4() {
		let mut ctx = EvalContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::testing_empty(),
				expression: Box::new(Constant(Number {
					fragment: Fragment::internal("-1.1"),
				})),
				to: TypeExpression {
					fragment: Fragment::testing_empty(),
					ty: Type::Float4,
				},
			}),
			&Functions::empty(),
			&Clock::default(),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::float4([-1.1]));
	}

	#[test]
	fn test_cast_negative_float_8() {
		let mut ctx = EvalContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::testing_empty(),
				expression: Box::new(Constant(Number {
					fragment: Fragment::internal("-1.1"),
				})),
				to: TypeExpression {
					fragment: Fragment::testing_empty(),
					ty: Type::Float8,
				},
			}),
			&Functions::empty(),
			&Clock::default(),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::float8([-1.1]));
	}

	#[test]
	fn test_cast_string_to_bool() {
		let mut ctx = EvalContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::testing_empty(),
				expression: Box::new(Constant(ConstantExpression::Text {
					fragment: Fragment::internal("0"),
				})),
				to: TypeExpression {
					fragment: Fragment::testing_empty(),
					ty: Type::Boolean,
				},
			}),
			&Functions::empty(),
			&Clock::default(),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::bool([false]));
	}

	#[test]
	fn test_cast_string_neg_one_to_bool_should_fail() {
		let mut ctx = EvalContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::testing_empty(),
				expression: Box::new(Constant(ConstantExpression::Text {
					fragment: Fragment::internal("-1"),
				})),
				to: TypeExpression {
					fragment: Fragment::testing_empty(),
					ty: Type::Boolean,
				},
			}),
			&Functions::empty(),
			&Clock::default(),
		);

		assert!(result.is_err());

		// Check that the error is the expected CAST_004
		// (invalid_boolean) error
		let err = result.unwrap_err();
		let diagnostic = err.0;
		assert_eq!(diagnostic.code, "CAST_004");
		assert!(diagnostic.cause.is_some());
		let cause = diagnostic.cause.unwrap();
		assert_eq!(cause.code, "BOOLEAN_003"); // invalid_number_boolean
	}

	#[test]
	fn test_cast_boolean_to_date_should_fail() {
		let mut ctx = EvalContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::testing_empty(),
				expression: Box::new(Constant(ConstantExpression::Bool {
					fragment: Fragment::internal("true"),
				})),
				to: TypeExpression {
					fragment: Fragment::testing_empty(),
					ty: Type::Date,
				},
			}),
			&Functions::empty(),
			&Clock::default(),
		);

		assert!(result.is_err());

		// Check that the error is the expected CAST_001
		// (unsupported_cast) error
		let err = result.unwrap_err();
		let diagnostic = err.0;
		assert_eq!(diagnostic.code, "CAST_001");
	}

	#[test]
	fn test_cast_text_to_decimal() {
		let mut ctx = EvalContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::testing_empty(),
				expression: Box::new(Constant(ConstantExpression::Text {
					fragment: Fragment::internal("123.456789"),
				})),
				to: TypeExpression {
					fragment: Fragment::testing_empty(),
					ty: Type::Decimal,
				},
			}),
			&Functions::empty(),
			&Clock::default(),
		)
		.unwrap();

		if let ColumnData::Decimal {
			container,
			..
		} = result.data()
		{
			assert_eq!(container.len(), 1);
			assert!(container.is_defined(0));
			let value = &container[0];
			assert_eq!(value.to_string(), "123.456789");
		} else {
			panic!("Expected Decimal column data");
		}
	}
}
