// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::value::column::{ColumnWithName, cast::cast_column_data};
use reifydb_rql::expression::Expression;

use crate::{
	Result,
	expression::{
		compile::compile_expression,
		context::{CompileContext, EvalContext},
	},
};

pub fn evaluate(ctx: &EvalContext, expr: &Expression) -> Result<ColumnWithName> {
	let compile_ctx = CompileContext {
		symbols: ctx.symbols,
	};
	let compiled = compile_expression(&compile_ctx, expr)?;
	let column = compiled.execute(ctx)?;

	if let Some(ty) = ctx.target.as_ref().map(|c| c.column_type()) {
		let data = cast_column_data(ctx, column.data(), ty, &expr.lazy_fragment())?;
		Ok(ColumnWithName {
			name: column.name,
			data,
		})
	} else {
		Ok(column)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::value::column::buffer::ColumnBuffer;
	use reifydb_rql::expression::{
		CastExpression, ConstantExpression,
		ConstantExpression::Number,
		Expression::{Cast, Constant, Prefix},
		PrefixExpression, PrefixOperator, TypeExpression,
	};
	use reifydb_value::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, value_type::ValueType},
	};

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
					ty: TypeConstraint::unconstrained(ValueType::Int4),
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnBuffer::int4([42]));
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
					ty: TypeConstraint::unconstrained(ValueType::Int4),
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnBuffer::int4([-42]));
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
					ty: TypeConstraint::unconstrained(ValueType::Int1),
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnBuffer::int1([-128]));
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
					ty: TypeConstraint::unconstrained(ValueType::Float8),
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnBuffer::float8([4.2]));
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
					ty: TypeConstraint::unconstrained(ValueType::Float4),
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnBuffer::float4([4.2]));
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
					ty: TypeConstraint::unconstrained(ValueType::Float4),
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnBuffer::float4([-1.1]));
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
					ty: TypeConstraint::unconstrained(ValueType::Float8),
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnBuffer::float8([-1.1]));
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
					ty: TypeConstraint::unconstrained(ValueType::Boolean),
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnBuffer::bool([false]));
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
					ty: TypeConstraint::unconstrained(ValueType::Boolean),
				},
			}),
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
					ty: TypeConstraint::unconstrained(ValueType::Date),
				},
			}),
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
					ty: TypeConstraint::unconstrained(ValueType::Decimal),
				},
			}),
		)
		.unwrap();

		if let ColumnBuffer::Decimal {
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
