// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod number;
mod temporal;
mod text;
mod uuid;

use number::NumberParser;
use reifydb_core::{
	Type,
	interface::evaluate::expression::ConstantExpression,
	interface::fragment::IntoFragment,
	result::error::diagnostic::cast,
	return_error,
	value::{
		boolean::parse_bool,
		container::undefined::UndefinedContainer,
		number::{parse_float, parse_int, parse_uint},
	},
};
use temporal::TemporalParser;
use text::TextParser;

use crate::{
	columnar::{Column, ColumnData, ColumnQualified},
	evaluate::{EvaluationContext, Evaluator},
};

impl Evaluator {
	pub(crate) fn constant(
		&self,
		ctx: &EvaluationContext,
		expr: &ConstantExpression,
	) -> crate::Result<Column> {
		let row_count = ctx.take.unwrap_or(ctx.row_count);
		Ok(Column::ColumnQualified(ColumnQualified {
			name: expr.fragment().fragment().into(),
			data: Self::constant_value(&expr, row_count)?,
		}))
	}

	pub(crate) fn constant_of(
		&self,
		ctx: &EvaluationContext,
		expr: &ConstantExpression,
		target: Type,
	) -> crate::Result<Column> {
		let row_count = ctx.take.unwrap_or(ctx.row_count);
		let data = Self::constant_value(&expr, row_count)?;
		let casted = {
			let source = data.get_type();
			if source == target {
				data
			} else {
				Self::constant_value_of(
					&expr, target, row_count,
				)?
			}
		};
		Ok(Column::ColumnQualified(ColumnQualified {
			name: expr.fragment().fragment().into(),
			data: casted,
		}))
	}

	fn constant_value(
		expr: &ConstantExpression,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		Ok(match expr {
			ConstantExpression::Bool {
				fragment,
			} => match parse_bool(fragment.clone()) {
				Ok(v) => {
					return Ok(ColumnData::bool(
						vec![v; row_count],
					));
				}
				Err(err) => return_error!(err.diagnostic()),
			},
			ConstantExpression::Number {
				fragment,
			} => {
				if fragment.fragment().contains(".")
					|| fragment.fragment().contains("e")
				{
					return match parse_float(fragment.clone()) {
						Ok(v) => {
							Ok(ColumnData::float8(
								vec![
									v;
									row_count
								],
							))
						}
						Err(err) => return_error!(
							err.diagnostic()
						),
					};
				}

				if let Ok(v) = parse_int::<i8>(fragment.clone().into_fragment()) {
					return Ok(ColumnData::int1(
						vec![v; row_count],
					));
				}

				if let Ok(v) = parse_int::<i16>(fragment.clone().into_fragment()) {
					return Ok(ColumnData::int2(
						vec![v; row_count],
					));
				}

				if let Ok(v) = parse_int::<i32>(fragment.clone().into_fragment()) {
					return Ok(ColumnData::int4(
						vec![v; row_count],
					));
				}

				if let Ok(v) = parse_int::<i64>(fragment.clone().into_fragment()) {
					return Ok(ColumnData::int8(
						vec![v; row_count],
					));
				}

				// if parsing as i128 fails and its a negative
				// number, we are maxed out and can stop
				match parse_int::<i128>(fragment.clone().into_fragment()) {
					Ok(v) => {
						return Ok(ColumnData::int16(
							vec![v; row_count],
						));
					}
					Err(err) => {
						if fragment.fragment()
							.starts_with("-")
						{
							return Err(err);
						}
					}
				}

				return match parse_uint::<u128>(fragment.clone()) {
					Ok(v) => Ok(ColumnData::uint16(
						vec![v; row_count],
					)),
					Err(err) => {
						return_error!(err.diagnostic());
					}
				};
			}
			ConstantExpression::Text {
				fragment,
			} => ColumnData::utf8(
				std::iter::repeat(fragment.fragment().clone())
					.take(row_count),
			),
			ConstantExpression::Temporal {
				fragment,
			} => TemporalParser::parse_temporal(
				fragment.clone().into_fragment(),
				row_count,
			)?,
			ConstantExpression::Undefined {
				..
			} => ColumnData::Undefined(UndefinedContainer::new(
				row_count,
			)),
		})
	}

	fn constant_value_of(
		expr: &ConstantExpression,
		target: Type,
		row_count: usize,
	) -> crate::Result<ColumnData> {
		Ok(match (expr, target) {
			(
				ConstantExpression::Number {
					fragment,
				},
				target,
			) => NumberParser::from_number(
				fragment.clone().into_fragment(),
				target,
				row_count,
			)?,
			(
				ConstantExpression::Text {
					fragment,
				},
				target,
			) if target.is_bool()
				|| target.is_number() || target.is_temporal()
				|| target.is_uuid() =>
			{
				TextParser::from_text(
					fragment.clone().into_fragment(),
					target,
					row_count,
				)?
			}
			(
				ConstantExpression::Temporal {
					fragment,
				},
				target,
			) if target.is_temporal() => TemporalParser::from_temporal(
				fragment.clone().into_fragment(),
				target,
				row_count,
			)?,

			(_, target) => {
				let source_type = match expr {
					ConstantExpression::Bool {
						..
					} => Type::Bool,
					ConstantExpression::Number {
						..
					} => Type::Float8,
					ConstantExpression::Text {
						..
					} => Type::Utf8,
					ConstantExpression::Temporal {
						..
					} => Type::DateTime,
					ConstantExpression::Undefined {
						..
					} => Type::Undefined,
				};
				return_error!(cast::unsupported_cast(
					expr.fragment(),
					source_type,
					target
				));
			}
		})
	}
}
