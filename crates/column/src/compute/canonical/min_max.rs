// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_arith::aggregate::{max, min};
use arrow_array::Array;
use reifydb_core::value::column::data::canonical::Canonical;
use reifydb_value::{
	Result,
	value::{Value, column_view::ViewData, container::wide_int_array::wides},
};

use crate::error::ColumnError;

pub fn min_max(array: &Canonical) -> Result<(Value, Value)> {
	if array.is_empty() {
		return Err(ColumnError::MinMaxEmpty.into());
	}

	let view = array.view();
	let nulls = array.buffer().logical_nulls();
	let skip = |row: usize| -> bool { nulls.as_ref().map(|n| n.is_null(row)).unwrap_or(false) };

	macro_rules! reduce_arrow {
		($array:expr, $variant:ident) => {{
			match (min($array), max($array)) {
				(Some(low), Some(high)) => Ok((Value::$variant(low), Value::$variant(high))),
				_ => Err(ColumnError::MinMaxAllNone.into()),
			}
		}};
	}

	macro_rules! reduce_int {
		($slice:expr, $variant:ident) => {{
			let mut min = None;
			let mut max = None;
			for (i, &x) in $slice.iter().enumerate() {
				if skip(i) {
					continue;
				}
				min = Some(min.map_or(x, |m: _| {
					if x < m {
						x
					} else {
						m
					}
				}));
				max = Some(max.map_or(x, |m: _| {
					if x > m {
						x
					} else {
						m
					}
				}));
			}
			match (min, max) {
				(Some(min), Some(max)) => Ok((Value::$variant(min), Value::$variant(max))),
				_ => Err(ColumnError::MinMaxAllNone.into()),
			}
		}};
	}

	macro_rules! reduce_family {
		($array:expr) => {{
			let values = $array.unscaled_values();
			let mut low: Option<usize> = None;
			let mut high: Option<usize> = None;
			for (i, &x) in values.iter().enumerate() {
				if skip(i) {
					continue;
				}
				if low.is_none_or(|m| x < values[m]) {
					low = Some(i);
				}
				if high.is_none_or(|m| x > values[m]) {
					high = Some(i);
				}
			}
			match (low, high) {
				(Some(low), Some(high)) => Ok((view.get_value(low), view.get_value(high))),
				_ => Err(ColumnError::MinMaxAllNone.into()),
			}
		}};
	}

	match &view.data {
		ViewData::Int1(c) => reduce_arrow!(*c, Int1),
		ViewData::Int2(c) => reduce_arrow!(*c, Int2),
		ViewData::Int4(c) => reduce_arrow!(*c, Int4),
		ViewData::Int8(c) => reduce_arrow!(*c, Int8),
		ViewData::Int16(c) => reduce_int!(wides::<i128>(c), Int16),
		ViewData::Uint1(c) => reduce_arrow!(*c, Uint1),
		ViewData::Uint2(c) => reduce_arrow!(*c, Uint2),
		ViewData::Uint4(c) => reduce_arrow!(*c, Uint4),
		ViewData::Uint8(c) => reduce_arrow!(*c, Uint8),
		ViewData::Uint16(c) => reduce_int!(wides::<u128>(c), Uint16),
		ViewData::Decimal(c) => reduce_family!(c),
		ViewData::Any {
			..
		} => Err(ColumnError::FixedArrayRequired {
			operation: "min_max",
		}
		.into()),
		_ => reduce_ordered(array),
	}
}

fn reduce_ordered(array: &Canonical) -> Result<(Value, Value)> {
	let view = array.view();
	let nulls = array.buffer().logical_nulls();
	let mut min: Option<Value> = None;
	let mut max: Option<Value> = None;
	for row in 0..array.len() {
		if nulls.as_ref().map(|n| n.is_null(row)).unwrap_or(false) {
			continue;
		}
		let value = view.get_value(row);
		if min.as_ref().is_none_or(|current| value < *current) {
			min = Some(value.clone());
		}
		if max.as_ref().is_none_or(|current| value > *current) {
			max = Some(value);
		}
	}
	match (min, max) {
		(Some(min), Some(max)) => Ok((min, max)),
		_ => Err(ColumnError::MinMaxAllNone.into()),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::value::column::{builder::ColumnBuilder, factory};
	use reifydb_value::value::{
		constraint::{precision::Precision, scale::Scale},
		decimal::Decimal,
		value_type::ValueType,
	};

	use super::*;

	#[test]
	fn min_max_over_int4() {
		let ca = Canonical::from_column(&factory::int4("c", [30i32, 10, 50, 20, 40])).unwrap();
		let (min, max) = min_max(&ca).unwrap();
		assert_eq!(min, Value::Int4(10));
		assert_eq!(max, Value::Int4(50));
	}

	#[test]
	fn min_max_uint16_above_u64() {
		// A lossy i256 cast collapses rows above u64 MAX to none or equal values and picks the wrong bounds.
		let ca = Canonical::from_column(&factory::uint16("c", [u128::MAX, (1u128 << 64) + 1, 1u128 << 64]))
			.unwrap();
		let (min, max) = min_max(&ca).unwrap();
		assert_eq!(min, Value::Uint16(1u128 << 64));
		assert_eq!(max, Value::Uint16(u128::MAX));
	}

	#[test]
	fn min_max_int16_at_i128_bounds() {
		// A truncating read of the i128 rows breaks the order at the extremes and picks the wrong bounds.
		let ca = Canonical::from_column(&factory::int16("c", [0i128, i128::MAX, -1, i128::MIN, 1])).unwrap();
		let (min, max) = min_max(&ca).unwrap();
		assert_eq!(min, Value::Int16(i128::MIN));
		assert_eq!(max, Value::Int16(i128::MAX));
	}

	#[test]
	fn min_max_decimal_skips_nones_and_keeps_the_column_scale() {
		// The bounds must come back at the column scale, and a none row must never win as zero.
		let d = |t: &str| Decimal::parse(t).unwrap();
		let cd = factory::decimal_with_bitvec(
			"c",
			Precision::new(10),
			Scale::new(2),
			[d("1.50"), d("0.00"), d("-2.25"), d("0.00"), d("3.10")],
			vec![true, false, true, false, true],
		);
		let ca = Canonical::from_column(&cd).unwrap();
		let (min, max) = min_max(&ca).unwrap();
		assert_eq!(min.to_string(), "-2.25");
		assert_eq!(max.to_string(), "3.10");
		assert!(matches!(&min, Value::Decimal(v) if v.scale() == 2));
	}

	#[test]
	fn min_max_skips_nones() {
		let mut cd = ColumnBuilder::with_capacity(ValueType::Int4, 5);
		cd.push::<i32>(30);
		cd.push_none();
		cd.push::<i32>(10);
		cd.push_none();
		cd.push::<i32>(50);
		let ca = Canonical::from_column(&cd.finish("c")).unwrap();
		let (min, max) = min_max(&ca).unwrap();
		assert_eq!(min, Value::Int4(10));
		assert_eq!(max, Value::Int4(50));
	}
}
