// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod temporal;

use reifydb_core::value::column::buffer::ColumnBuffer;
use reifydb_rql::expression::ConstantExpression;
use reifydb_value::{
	fragment::Fragment,
	return_error,
	value::{
		boolean::parse::parse_bool,
		constraint::{precision::Precision, scale::Scale},
		decimal::parse::parse_decimal,
		number::parse::{parse_primitive_int, parse_primitive_uint},
		temporal::parse::duration::parse_duration,
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
		} => number_value(fragment, row_count)?,
		ConstantExpression::Text {
			fragment,
		} => ColumnBuffer::utf8_repeated(fragment.text(), row_count),
		ConstantExpression::Temporal {
			fragment,
		} => TemporalParser::parse_temporal(fragment.clone(), row_count)?,
		ConstantExpression::Duration {
			fragment,
		} => ColumnBuffer::duration(vec![parse_duration(fragment.clone())?; row_count]),
		ConstantExpression::None {
			..
		} => ColumnBuffer::none(row_count),
	})
}

fn number_value(fragment: &Fragment, row_count: usize) -> Result<ColumnBuffer> {
	let text = fragment.text();
	if text.contains(['.', 'e', 'E']) {
		let value = parse_decimal(fragment.clone())?;
		let precision = Precision::new(value.digits().max(value.scale()));
		let scale = Scale::new(value.scale());
		return Ok(ColumnBuffer::decimal(precision, scale, vec![value; row_count]));
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
	if let Ok(v) = parse_primitive_int::<i128>(fragment.clone()) {
		return Ok(ColumnBuffer::int16(vec![v; row_count]));
	}
	if let Ok(v) = parse_primitive_uint::<u128>(fragment.clone()) {
		return Ok(ColumnBuffer::uint16(vec![v; row_count]));
	}
	let value = parse_decimal(fragment.clone())?;
	let precision = Precision::new(value.digits().max(value.scale()));
	let scale = Scale::new(value.scale());
	Ok(ColumnBuffer::decimal(precision, scale, vec![value; row_count]))
}

#[cfg(test)]
mod tests {
	use reifydb_rql::expression::ConstantExpression;
	use reifydb_value::fragment::Fragment;

	use super::constant_value;

	#[test]
	fn a_none_literal_is_a_none_column() {
		// Otherwise a none literal is an any column and a typed branch beside it can not merge with it.
		let none = ConstantExpression::None {
			fragment: Fragment::internal("none"),
		};

		let column = constant_value(&none, 3).unwrap();

		assert!(column.is_none(), "got {:?}", column.get_type());
		assert_eq!(column.len(), 3);
	}
}
