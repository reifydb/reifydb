// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod number;
pub mod temporal;
pub mod text;
pub mod uuid;

use number::NumberParser;
use reifydb_core::value::column::data::ColumnData;
use reifydb_rql::expression::ConstantExpression;
use reifydb_type::{
	error::diagnostic::cast,
	return_error,
	value::{
		boolean::parse::parse_bool,
		number::parse::{parse_float, parse_primitive_int, parse_primitive_uint},
		r#type::Type,
	},
};
use temporal::TemporalParser;
use text::TextParser;

pub(crate) fn constant_value(expr: &ConstantExpression, row_count: usize) -> crate::Result<ColumnData> {
	Ok(match expr {
		ConstantExpression::Bool {
			fragment,
		} => match parse_bool(fragment.clone()) {
			Ok(v) => {
				return Ok(ColumnData::bool(vec![v; row_count]));
			}
			Err(err) => return_error!(err.diagnostic()),
		},
		ConstantExpression::Number {
			fragment,
		} => {
			if fragment.text().contains(".") || fragment.text().contains("e") {
				return match parse_float(fragment.clone()) {
					Ok(v) => Ok(ColumnData::float8(vec![v; row_count])),
					Err(err) => return_error!(err.diagnostic()),
				};
			}

			if let Ok(v) = parse_primitive_int::<i8>(fragment.clone()) {
				return Ok(ColumnData::int1(vec![v; row_count]));
			}

			if let Ok(v) = parse_primitive_int::<i16>(fragment.clone()) {
				return Ok(ColumnData::int2(vec![v; row_count]));
			}

			if let Ok(v) = parse_primitive_int::<i32>(fragment.clone()) {
				return Ok(ColumnData::int4(vec![v; row_count]));
			}

			if let Ok(v) = parse_primitive_int::<i64>(fragment.clone()) {
				return Ok(ColumnData::int8(vec![v; row_count]));
			}

			// if parsing as i128 fails and its a negative
			// number, we are maxed out and can stop
			match parse_primitive_int::<i128>(fragment.clone()) {
				Ok(v) => {
					return Ok(ColumnData::int16(vec![v; row_count]));
				}
				Err(err) => {
					if fragment.text().starts_with("-") {
						return Err(err);
					}
				}
			}

			return match parse_primitive_uint::<u128>(fragment.clone()) {
				Ok(v) => Ok(ColumnData::uint16(vec![v; row_count])),
				Err(err) => {
					return_error!(err.diagnostic());
				}
			};
		}
		ConstantExpression::Text {
			fragment,
		} => ColumnData::utf8(std::iter::repeat(fragment.text()).take(row_count)),
		ConstantExpression::Temporal {
			fragment,
		} => TemporalParser::parse_temporal(fragment.clone(), row_count)?,
		ConstantExpression::None {
			..
		} => ColumnData::none_typed(Type::Any, row_count),
	})
}

pub(crate) fn constant_value_of(
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
		) => NumberParser::from_number(fragment.clone(), target, row_count)?,
		(
			ConstantExpression::Text {
				fragment,
			},
			target,
		) if target.is_bool() || target.is_number() || target.is_temporal() || target.is_uuid() => {
			TextParser::from_text(fragment.clone(), target, row_count)?
		}
		(
			ConstantExpression::Temporal {
				fragment,
			},
			target,
		) if target.is_temporal() => TemporalParser::from_temporal(fragment.clone(), target, row_count)?,

		(
			ConstantExpression::None {
				..
			},
			target,
		) => ColumnData::none_typed(target, row_count),

		(_, target) => {
			let source_type = match expr {
				ConstantExpression::Bool {
					..
				} => Type::Boolean,
				ConstantExpression::Number {
					..
				} => Type::Float8,
				ConstantExpression::Text {
					..
				} => Type::Utf8,
				ConstantExpression::Temporal {
					..
				} => Type::DateTime,
				ConstantExpression::None {
					..
				} => Type::Option(Box::new(Type::Any)),
			};
			return_error!(cast::unsupported_cast(expr.full_fragment_owned(), source_type, target));
		}
	})
}
