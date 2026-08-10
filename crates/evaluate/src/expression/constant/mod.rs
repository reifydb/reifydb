// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod temporal;

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_rql::expression::ConstantExpression;
use reifydb_value::{
	return_error,
	value::{
		boolean::parse::parse_bool,
		decimal::parse::parse_decimal,
		int::Int,
		number::parse::{parse_primitive_int, parse_primitive_uint},
		value_type::ValueType,
	},
};
use temporal::TemporalParser;

use crate::Result;

pub(crate) fn constant_value(expr: &ConstantExpression, row_count: usize) -> Result<ColumnBuffer> {
	Ok(match expr {
		ConstantExpression::Bool {
			fragment,
		} => match parse_bool(fragment.clone()) {
			Ok(v) => {
				return Ok(ColumnBuffer::bool(vec![v; row_count]));
			}
			Err(err) => return_error!(err.diagnostic()),
		},
		ConstantExpression::Number {
			fragment,
		} => {
			if fragment.text().contains(".") || fragment.text().contains("e") {
				return match parse_decimal(fragment.clone()) {
					Ok(v) => Ok(ColumnBuffer::decimal(vec![v; row_count])),
					Err(err) => return_error!(err.diagnostic()),
				};
			}

			if let Ok(v) = parse_primitive_int::<i8>(fragment.clone()) {
				return Ok(ColumnBuffer::int1(vec![v; row_count]));
			}

			if let Ok(v) = parse_primitive_int::<i16>(fragment.clone()) {
				return Ok(ColumnBuffer::int2(vec![v; row_count]));
			}

			if let Ok(v) = parse_primitive_int::<i32>(fragment.clone()) {
				return Ok(ColumnBuffer::int4(vec![v; row_count]));
			}

			if let Ok(v) = parse_primitive_int::<i64>(fragment.clone()) {
				return Ok(ColumnBuffer::int8(vec![v; row_count]));
			}

			match parse_primitive_int::<i128>(fragment.clone()) {
				Ok(v) => {
					return Ok(ColumnBuffer::int16(vec![v; row_count]));
				}
				Err(err) => {
					if fragment.text().starts_with("-") {
						return Err(err);
					}
				}
			}

			if let Ok(v) = parse_primitive_uint::<u128>(fragment.clone()) {
				return Ok(ColumnBuffer::uint16(vec![v; row_count]));
			}

			return match parse_primitive_int::<Int>(fragment.clone()) {
				Ok(v) => Ok(ColumnBuffer::int(vec![v; row_count])),
				Err(err) => {
					return_error!(err.diagnostic());
				}
			};
		}
		ConstantExpression::Text {
			fragment,
		} => ColumnBuffer::utf8_repeated(fragment.text(), row_count),
		ConstantExpression::Temporal {
			fragment,
		} => TemporalParser::parse_temporal(fragment.clone(), row_count)?,
		ConstantExpression::None {
			..
		} => ColumnBuffer::none_typed(ValueType::Any, row_count),
	})
}
