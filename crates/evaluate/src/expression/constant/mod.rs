// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod temporal;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{expression::ConstantExpression, value::column::factory};
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

pub(crate) fn constant_value(expr: &ConstantExpression, name: &str, row_count: usize) -> Result<(FieldRef, ArrayRef)> {
	Ok(match expr {
		ConstantExpression::Bool {
			fragment,
		} => match parse_bool(fragment.clone()) {
			Ok(v) => {
				return Ok(factory::bool(name, vec![v; row_count]));
			}
			Err(err) => return_error!(err.diagnostic()),
		},
		ConstantExpression::Number {
			fragment,
		} => number_value(fragment, name, row_count)?,
		ConstantExpression::Text {
			fragment,
		} => factory::utf8_repeated(name, fragment.text(), row_count),
		ConstantExpression::Temporal {
			fragment,
		} => TemporalParser::parse_temporal(fragment.clone(), name, row_count)?,
		ConstantExpression::Duration {
			fragment,
		} => factory::duration(name, vec![parse_duration(fragment.clone())?; row_count]),
		ConstantExpression::None {
			..
		} => factory::none(name, row_count),
	})
}

fn number_value(fragment: &Fragment, name: &str, row_count: usize) -> Result<(FieldRef, ArrayRef)> {
	let text = fragment.text();
	if text.contains(['.', 'e', 'E']) {
		let value = parse_decimal(fragment.clone())?;
		let precision = Precision::new(value.digits().max(value.scale()));
		let scale = Scale::new(value.scale());
		return Ok(factory::decimal(name, precision, scale, vec![value; row_count]));
	}
	if let Ok(v) = parse_primitive_int::<i8>(fragment.clone()) {
		return Ok(factory::int1(name, vec![v; row_count]));
	}
	if let Ok(v) = parse_primitive_int::<i16>(fragment.clone()) {
		return Ok(factory::int2(name, vec![v; row_count]));
	}
	if let Ok(v) = parse_primitive_int::<i32>(fragment.clone()) {
		return Ok(factory::int4(name, vec![v; row_count]));
	}
	if let Ok(v) = parse_primitive_int::<i64>(fragment.clone()) {
		return Ok(factory::int8(name, vec![v; row_count]));
	}
	if let Ok(v) = parse_primitive_int::<i128>(fragment.clone()) {
		return Ok(factory::int16(name, vec![v; row_count]));
	}
	if let Ok(v) = parse_primitive_uint::<u128>(fragment.clone()) {
		return Ok(factory::uint16(name, vec![v; row_count]));
	}
	let value = parse_decimal(fragment.clone())?;
	let precision = Precision::new(value.digits().max(value.scale()));
	let scale = Scale::new(value.scale());
	Ok(factory::decimal(name, precision, scale, vec![value; row_count]))
}

#[cfg(test)]
mod tests {
	use reifydb_core::expression::ConstantExpression;
	use reifydb_value::{fragment::Fragment, value::column_view::ColumnView};

	use super::constant_value;

	#[test]
	fn a_none_literal_is_a_none_column() {
		// Otherwise a none literal is an any column and a typed branch beside it can not merge with it.
		let none = ConstantExpression::None {
			fragment: Fragment::internal("none"),
		};

		let column = constant_value(&none, "none", 3).unwrap();
		let view = ColumnView::try_from(&column).unwrap();

		assert!(view.is_none(), "got {:?}", view.get_type());
		assert_eq!(view.len(), 3);
	}
}
