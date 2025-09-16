// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

pub mod blob;
pub mod boolean;
pub mod number;
pub mod temporal;
pub mod text;
pub mod uuid;

use std::ops::Deref;

use reifydb_core::{
	interface::{
		Evaluator, LazyFragment,
		expression::{CastExpression, Expression},
	},
	value::columnar::{Column, ColumnData, ColumnQualified, SourceQualified},
};
use reifydb_type::{Type, diagnostic::cast, err, error};

use crate::evaluate::{EvaluationContext, StandardEvaluator};

impl StandardEvaluator {
	pub(crate) fn cast(&self, ctx: &EvaluationContext, cast: &CastExpression) -> crate::Result<Column> {
		let cast_fragment = cast.lazy_fragment();

		// FIXME optimization does not apply for prefix expressions,
		// like cast(-2 as int1) at the moment
		match cast.expression.deref() {
			// Optimization: it is a constant value and we now the
			// target ty, therefore it is possible to create the
			// data directly which means there is no reason to
			// further adjust the column
			Expression::Constant(expr) => self.constant_of(ctx, expr, cast.to.ty),
			expr => {
				let column = self.evaluate(ctx, expr)?;
				let lazy_frag = cast.expression.lazy_fragment();
				let casted =
					cast_column_data(ctx, &column.data(), cast.to.ty, &lazy_frag).map_err(|e| {
						error!(cast::invalid_number(
							cast_fragment().into_owned(),
							cast.to.ty,
							e.diagnostic()
						))
					})?;

				Ok(match column.table() {
					Some(source) => Column::SourceQualified(SourceQualified {
						source: source.to_string(),
						name: column.name().to_string(),
						data: casted,
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().to_string(),
						data: casted,
					}),
				})
			}
		}
	}
}

pub fn cast_column_data<'a>(
	ctx: &EvaluationContext,
	data: &ColumnData,
	target: Type,
	lazy_fragment: impl LazyFragment<'a>,
) -> crate::Result<ColumnData> {
	if let ColumnData::Undefined(container) = data {
		let mut result = ColumnData::with_capacity(target, container.len());
		for _ in 0..container.len() {
			result.push_undefined();
		}
		return Ok(result);
	}
	let source_type = data.get_type();
	match (source_type, target) {
		_ if target == source_type => Ok(data.clone()),
		(_, target) if target.is_number() => number::to_number(ctx, data, target, lazy_fragment),
		(_, target) if target.is_blob() => blob::to_blob(data, lazy_fragment),
		(_, target) if target.is_bool() => boolean::to_boolean(data, lazy_fragment),
		(_, target) if target.is_utf8() => text::to_text(data, lazy_fragment),
		(_, target) if target.is_temporal() => temporal::to_temporal(data, target, lazy_fragment),
		(_, target) if target.is_uuid() => uuid::to_uuid(data, target, lazy_fragment),
		(source, target) if source.is_uuid() || target.is_uuid() => uuid::to_uuid(data, target, lazy_fragment),
		_ => {
			err!(cast::unsupported_cast(lazy_fragment.fragment(), source_type, target))
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::{
			EvaluationContext,
			expression::{
				CastExpression, ConstantExpression,
				ConstantExpression::Number,
				Expression::{Cast, Constant, Prefix},
				PrefixExpression, PrefixOperator, TypeExpression,
			},
		},
		value::columnar::ColumnData,
	};
	use reifydb_type::{Fragment, Type};

	use crate::evaluate::evaluate;

	#[test]
	fn test_cast_integer() {
		let mut ctx = EvaluationContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::owned_empty(),
				expression: Box::new(Constant(Number {
					fragment: Fragment::owned_internal("42"),
				})),
				to: TypeExpression {
					fragment: Fragment::owned_empty(),
					ty: Type::Int4,
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::int4([42]));
	}

	#[test]
	fn test_cast_negative_integer() {
		let mut ctx = EvaluationContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::owned_empty(),
				expression: Box::new(Prefix(PrefixExpression {
					operator: PrefixOperator::Minus(Fragment::owned_empty()),
					expression: Box::new(Constant(Number {
						fragment: Fragment::owned_internal("42"),
					})),
					fragment: Fragment::owned_empty(),
				})),
				to: TypeExpression {
					fragment: Fragment::owned_empty(),
					ty: Type::Int4,
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::int4([-42]));
	}

	#[test]
	fn test_cast_negative_min() {
		let mut ctx = EvaluationContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::owned_empty(),
				expression: Box::new(Prefix(PrefixExpression {
					operator: PrefixOperator::Minus(Fragment::owned_empty()),
					expression: Box::new(Constant(Number {
						fragment: Fragment::owned_internal("128"),
					})),
					fragment: Fragment::owned_empty(),
				})),
				to: TypeExpression {
					fragment: Fragment::owned_empty(),
					ty: Type::Int1,
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::int1([-128]));
	}

	#[test]
	fn test_cast_float_8() {
		let mut ctx = EvaluationContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::owned_empty(),
				expression: Box::new(Constant(Number {
					fragment: Fragment::owned_internal("4.2"),
				})),
				to: TypeExpression {
					fragment: Fragment::owned_empty(),
					ty: Type::Float8,
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::float8([4.2]));
	}

	#[test]
	fn test_cast_float_4() {
		let mut ctx = EvaluationContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::owned_empty(),
				expression: Box::new(Constant(Number {
					fragment: Fragment::owned_internal("4.2"),
				})),
				to: TypeExpression {
					fragment: Fragment::owned_empty(),
					ty: Type::Float4,
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::float4([4.2]));
	}

	#[test]
	fn test_cast_negative_float_4() {
		let mut ctx = EvaluationContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::owned_empty(),
				expression: Box::new(Constant(Number {
					fragment: Fragment::owned_internal("-1.1"),
				})),
				to: TypeExpression {
					fragment: Fragment::owned_empty(),
					ty: Type::Float4,
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::float4([-1.1]));
	}

	#[test]
	fn test_cast_negative_float_8() {
		let mut ctx = EvaluationContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::owned_empty(),
				expression: Box::new(Constant(Number {
					fragment: Fragment::owned_internal("-1.1"),
				})),
				to: TypeExpression {
					fragment: Fragment::owned_empty(),
					ty: Type::Float8,
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::float8([-1.1]));
	}

	#[test]
	fn test_cast_string_to_bool() {
		let mut ctx = EvaluationContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::owned_empty(),
				expression: Box::new(Constant(ConstantExpression::Text {
					fragment: Fragment::owned_internal("0"),
				})),
				to: TypeExpression {
					fragment: Fragment::owned_empty(),
					ty: Type::Boolean,
				},
			}),
		)
		.unwrap();

		assert_eq!(*result.data(), ColumnData::bool([false]));
	}

	#[test]
	fn test_cast_string_neg_one_to_bool_should_fail() {
		let mut ctx = EvaluationContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::owned_empty(),
				expression: Box::new(Constant(ConstantExpression::Text {
					fragment: Fragment::owned_internal("-1"),
				})),
				to: TypeExpression {
					fragment: Fragment::owned_empty(),
					ty: Type::Boolean,
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
		let mut ctx = EvaluationContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::owned_empty(),
				expression: Box::new(Constant(ConstantExpression::Bool {
					fragment: Fragment::owned_internal("true"),
				})),
				to: TypeExpression {
					fragment: Fragment::owned_empty(),
					ty: Type::Date,
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
}
