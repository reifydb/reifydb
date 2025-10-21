// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

pub mod blob;
pub mod boolean;
pub mod number;
pub mod temporal;
pub mod text;
pub mod uuid;

use std::ops::Deref;

use reifydb_core::value::column::{Column, ColumnData};
use reifydb_rql::expression::{CastExpression, Expression};
use reifydb_type::{LazyFragment, Type, diagnostic::cast, err, error};

use crate::evaluate::column::{ColumnEvaluationContext, StandardColumnEvaluator};

impl StandardColumnEvaluator {
	pub(crate) fn cast<'a>(
		&self,
		ctx: &ColumnEvaluationContext<'a>,
		cast: &CastExpression<'a>,
	) -> crate::Result<Column<'a>> {
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

				Ok(Column {
					name: column.name_owned(),
					data: casted,
				})
			}
		}
	}
}

pub(crate) fn cast_column_data<'a>(
	ctx: &ColumnEvaluationContext,
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
	use reifydb_core::value::column::ColumnData;
	use reifydb_rql::expression::{
		CastExpression, ConstantExpression,
		ConstantExpression::Number,
		Expression::{Cast, Constant, Prefix},
		PrefixExpression, PrefixOperator, TypeExpression,
	};
	use reifydb_type::{Fragment, Type};

	use crate::evaluate::{ColumnEvaluationContext, column::evaluate};

	#[test]
	fn test_cast_integer() {
		let mut ctx = ColumnEvaluationContext::testing();
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
		let mut ctx = ColumnEvaluationContext::testing();
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
		let mut ctx = ColumnEvaluationContext::testing();
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
		let mut ctx = ColumnEvaluationContext::testing();
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
		let mut ctx = ColumnEvaluationContext::testing();
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
		let mut ctx = ColumnEvaluationContext::testing();
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
		let mut ctx = ColumnEvaluationContext::testing();
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
		let mut ctx = ColumnEvaluationContext::testing();
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
		let mut ctx = ColumnEvaluationContext::testing();
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
		let mut ctx = ColumnEvaluationContext::testing();
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

	#[test]
	fn test_cast_text_to_decimal() {
		let mut ctx = ColumnEvaluationContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				fragment: Fragment::owned_empty(),
				expression: Box::new(Constant(ConstantExpression::Text {
					fragment: Fragment::owned_internal("123.456789"),
				})),
				to: TypeExpression {
					fragment: Fragment::owned_empty(),
					ty: Type::Decimal,
				},
			}),
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
