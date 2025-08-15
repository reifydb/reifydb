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
	OwnedSpan, Type, err, error,
	interface::{
		Evaluate,
		expression::{CastExpression, Expression},
	},
	result::error::diagnostic::cast,
};

use crate::{
	columnar::{Column, ColumnData, ColumnQualified, SourceQualified},
	evaluate::{EvaluationContext, Evaluator},
};

impl Evaluator {
	pub(crate) fn cast(
		&self,
		ctx: &EvaluationContext,
		cast: &CastExpression,
	) -> crate::Result<Column> {
		let cast_span = cast.lazy_span();

		// FIXME optimization does not apply for prefix expressions,
		// like cast(-2 as int1) at the moment
		match cast.expression.deref() {
			// Optimization: it is a constant value and we now the
			// target ty, therefore it is possible to create the
			// data directly which means there is no reason to
			// further adjust the column
			Expression::Constant(expr) => {
				self.constant_of(ctx, expr, cast.to.ty)
			}
			expr => {
				let column = self.evaluate(ctx, expr)?;

				let casted = cast_column_data(
					ctx,
					&column.data(),
					cast.to.ty,
					cast.expression.lazy_span(),
				)
				.map_err(|e| {
					error!(cast::invalid_number(
						cast_span(),
						cast.to.ty,
						e.diagnostic()
					))
				})?;

				Ok(match column.table() {
					Some(source) => {
						Column::SourceQualified(
							SourceQualified {
								source: source
									.to_string(
									),
								name: column
									.name()
									.to_string(
									),
								data: casted,
							},
						)
					}
					None => Column::ColumnQualified(
						ColumnQualified {
							name: column
								.name()
								.to_string(),
							data: casted,
						},
					),
				})
			}
		}
	}
}

pub fn cast_column_data(
	ctx: &EvaluationContext,
	data: &ColumnData,
	target: Type,
	span: impl Fn() -> OwnedSpan,
) -> crate::Result<ColumnData> {
	if let ColumnData::Undefined(container) = data {
		let mut result =
			ColumnData::with_capacity(target, container.len());
		for _ in 0..container.len() {
			result.push_undefined();
		}
		return Ok(result);
	}
	let source_type = data.get_type();
	match (source_type, target) {
		_ if target == source_type => Ok(data.clone()),
		(_, target) if target.is_number() => {
			number::to_number(ctx, data, target, span)
		}
		(_, target) if target.is_blob() => blob::to_blob(data, span),
		(_, target) if target.is_bool() => {
			boolean::to_boolean(data, span)
		}
		(_, target) if target.is_utf8() => text::to_text(data, span),
		(_, target) if target.is_temporal() => {
			temporal::to_temporal(data, target, span)
		}
		(_, target) if target.is_uuid() => {
			uuid::to_uuid(data, target, span)
		}
		(source, target) if source.is_uuid() || target.is_uuid() => {
			uuid::to_uuid(data, target, span)
		}
		_ => {
			err!(cast::unsupported_cast(
				span(),
				source_type,
				target
			))
		}
	}
}

#[cfg(test)]
mod tests {
	use ConstantExpression::Number;
	use Expression::{Cast, Constant};
	use reifydb_core::{
		OwnedSpan, Type,
		interface::expression::{
			CastExpression, ConstantExpression, Expression::Prefix,
			PrefixExpression, PrefixOperator, TypeExpression,
		},
	};

	use crate::{
		columnar::ColumnData,
		evaluate::{EvaluationContext, Expression, evaluate},
	};

	#[test]
	fn test_cast_integer() {
		let mut ctx = EvaluationContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				span: OwnedSpan::testing_empty(),
				expression: Box::new(Constant(Number {
					span: OwnedSpan::testing("42"),
				})),
				to: TypeExpression {
					span: OwnedSpan::testing_empty(),
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
                span: OwnedSpan::testing_empty(),
                expression: Box::new(Prefix(PrefixExpression {
                    operator: PrefixOperator::Minus(OwnedSpan::testing_empty()),
                    expression: Box::new(Constant(Number { span: OwnedSpan::testing("42") })),
                    span: OwnedSpan::testing_empty(),
                })),
                to: TypeExpression { span: OwnedSpan::testing_empty(), ty: Type::Int4 },
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
                span: OwnedSpan::testing_empty(),
                expression: Box::new(Prefix(PrefixExpression {
                    operator: PrefixOperator::Minus(OwnedSpan::testing_empty()),
                    expression: Box::new(Constant(Number { span: OwnedSpan::testing("128") })),
                    span: OwnedSpan::testing_empty(),
                })),
                to: TypeExpression { span: OwnedSpan::testing_empty(), ty: Type::Int1 },
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
				span: OwnedSpan::testing_empty(),
				expression: Box::new(Constant(Number {
					span: OwnedSpan::testing("4.2"),
				})),
				to: TypeExpression {
					span: OwnedSpan::testing_empty(),
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
				span: OwnedSpan::testing_empty(),
				expression: Box::new(Constant(Number {
					span: OwnedSpan::testing("4.2"),
				})),
				to: TypeExpression {
					span: OwnedSpan::testing_empty(),
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
				span: OwnedSpan::testing_empty(),
				expression: Box::new(Constant(Number {
					span: OwnedSpan::testing("-1.1"),
				})),
				to: TypeExpression {
					span: OwnedSpan::testing_empty(),
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
				span: OwnedSpan::testing_empty(),
				expression: Box::new(Constant(Number {
					span: OwnedSpan::testing("-1.1"),
				})),
				to: TypeExpression {
					span: OwnedSpan::testing_empty(),
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
				span: OwnedSpan::testing_empty(),
				expression: Box::new(Constant(
					ConstantExpression::Text {
						span: OwnedSpan::testing("0"),
					},
				)),
				to: TypeExpression {
					span: OwnedSpan::testing_empty(),
					ty: Type::Bool,
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
				span: OwnedSpan::testing_empty(),
				expression: Box::new(Constant(
					ConstantExpression::Text {
						span: OwnedSpan::testing("-1"),
					},
				)),
				to: TypeExpression {
					span: OwnedSpan::testing_empty(),
					ty: Type::Bool,
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
	fn test_cast_bool_to_date_should_fail() {
		let mut ctx = EvaluationContext::testing();
		let result = evaluate(
			&mut ctx,
			&Cast(CastExpression {
				span: OwnedSpan::testing_empty(),
				expression: Box::new(Constant(
					ConstantExpression::Bool {
						span: OwnedSpan::testing(
							"true",
						),
					},
				)),
				to: TypeExpression {
					span: OwnedSpan::testing_empty(),
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
