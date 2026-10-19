// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::series::SeriesKey,
	value::batch::{is_scalar, scalar_value},
};
use reifydb_evaluate::stack::{SymbolTable, Variable};
use reifydb_rql::nodes::{PartitionKey, SeriesBound};
use reifydb_transaction::transaction::ScanLayout;
use reifydb_value::value::{Value, partition::Partition};

use crate::vm::volcano::partition_parameter::choose_partition;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeriesRange {
	pub partition: Option<Partition>,
	pub start: Option<u64>,
	pub end: Option<u64>,
}

pub(crate) fn choose_series_range(
	starts: &[SeriesBound],
	ends: &[SeriesBound],
	partition_keys: Option<&[PartitionKey]>,
	key: &SeriesKey,
	layout: ScanLayout,
	symbols: &SymbolTable,
) -> Option<SeriesRange> {
	if layout == ScanLayout::Column {
		return None;
	}
	let partition = match partition_keys {
		Some(keys) => Some(choose_partition(keys, layout, symbols)?),
		None => None,
	};
	let start = starts.iter().filter_map(|bound| resolve_series_bound(bound, key, symbols)).max();
	let end = ends
		.iter()
		.filter_map(|bound| resolve_series_bound(bound, key, symbols).and_then(|value| value.checked_add(1)))
		.min();
	Some(SeriesRange {
		partition,
		start,
		end,
	})
}

fn resolve_series_bound(bound: &SeriesBound, key: &SeriesKey, symbols: &SymbolTable) -> Option<u64> {
	let variable = match bound {
		SeriesBound::Constant(value) => return Some(*value),
		SeriesBound::Variable(variable) => variable,
	};
	let Some(Variable::Columns {
		batch,
	}) = symbols.get(variable.name())
	else {
		return None;
	};
	if !is_scalar(batch) {
		return None;
	}
	let value = scalar_value(batch).ok()?;
	let fits_key =
		match key {
			SeriesKey::DateTime {
				..
			} => matches!(value, Value::DateTime(_)),
			SeriesKey::Integer {
				..
			} => matches!(
				value,
				Value::Int1(_)
					| Value::Int2(_) | Value::Int4(_) | Value::Int8(_)
					| Value::Int16(_) | Value::Uint1(_) | Value::Uint2(_)
					| Value::Uint4(_) | Value::Uint8(_) | Value::Uint16(_)
			),
		};
	if !fits_key {
		return None;
	}
	key.key_to_u64(value)
}

#[cfg(test)]
mod tests {
	use reifydb_core::{expression::VariableExpression, interface::catalog::series::TimestampPrecision};
	use reifydb_value::{
		fragment::Fragment,
		value::{datetime::DateTime, value_type::ValueType},
	};

	use super::*;

	fn bound(text: &str) -> SeriesBound {
		SeriesBound::Variable(VariableExpression {
			fragment: Fragment::internal(text),
		})
	}

	fn symbols(values: Vec<(&str, Value)>) -> SymbolTable {
		let mut symbols = SymbolTable::new();
		for (name, value) in values {
			symbols.set(name.to_string(), Variable::scalar(value), false).expect("a fresh name");
		}
		symbols
	}

	fn integer_key() -> SeriesKey {
		SeriesKey::Integer {
			column: "ts".to_string(),
		}
	}

	fn range(start: Option<u64>, end: Option<u64>) -> Option<SeriesRange> {
		Some(SeriesRange {
			partition: None,
			start,
			end,
		})
	}

	#[test]
	fn variable_bounds_narrow_to_a_start_and_an_exclusive_end() {
		// The end is exclusive, so a bound of 9 must stop after key 9, never at it.
		let values = symbols(vec![("1", Value::Int4(3)), ("2", Value::Uint8(9))]);
		let chosen = choose_series_range(
			&[bound("$1")],
			&[bound("$2")],
			None,
			&integer_key(),
			ScanLayout::Row,
			&values,
		);

		assert_eq!(chosen, range(Some(3), Some(10)));
	}

	#[test]
	fn the_tightest_of_several_bounds_wins() {
		// Every bound holds at once, so the range must be the largest start and the smallest end.
		let values = symbols(vec![("1", Value::Int4(3)), ("2", Value::Int4(20))]);
		let chosen = choose_series_range(
			&[bound("$1"), SeriesBound::Constant(5)],
			&[bound("$2"), SeriesBound::Constant(30)],
			None,
			&integer_key(),
			ScanLayout::Row,
			&values,
		);

		assert_eq!(chosen, range(Some(5), Some(21)));
	}

	#[test]
	fn a_bound_that_is_not_a_key_value_is_dropped() {
		// A dropped bound only widens the scan; a wrong-type or negative bound must never narrow it.
		let starts = [bound("$1")];
		for value in [Value::Int4(-1), Value::Utf8("3".to_string()), Value::none(), Value::float8(3.0)] {
			let chosen = choose_series_range(
				&starts,
				&[],
				None,
				&integer_key(),
				ScanLayout::Row,
				&symbols(vec![("1", value.clone())]),
			);
			assert_eq!(chosen, range(None, None), "{value:?}");
		}
		let max_end = choose_series_range(
			&[],
			&[bound("$1")],
			None,
			&integer_key(),
			ScanLayout::Row,
			&symbols(vec![("1", Value::Uint8(u64::MAX))]),
		);
		assert_eq!(max_end, range(None, None));
	}

	#[test]
	fn a_datetime_bound_floors_to_the_key_precision() {
		// Rows inside the same second share a key, so a sub-second bound must keep that whole key.
		let key = SeriesKey::DateTime {
			column: "ts".to_string(),
			precision: TimestampPrecision::Second,
		};
		let at = DateTime::from_nanos(1_500_000_000);
		let values = symbols(vec![("1", Value::DateTime(at)), ("2", Value::Int4(1))]);

		let chosen = choose_series_range(&[bound("$1")], &[bound("$1")], None, &key, ScanLayout::Row, &values);
		assert_eq!(chosen, range(Some(1), Some(2)));

		let integer_on_datetime =
			choose_series_range(&[bound("$2")], &[], None, &key, ScanLayout::Row, &values);
		assert_eq!(integer_on_datetime, range(None, None));
	}

	#[test]
	fn an_unresolved_partition_and_a_column_layout_keep_the_planned_scan() {
		// A partitioned series reads no range without its partition, and a column layout refuses the narrowed
		// read.
		let keys = [PartitionKey::Variable {
			variable: VariableExpression {
				fragment: Fragment::internal("$9"),
			},
			ty: ValueType::Utf8,
		}];
		let values = symbols(vec![("1", Value::Int4(3))]);

		assert_eq!(
			choose_series_range(&[bound("$1")], &[], Some(&keys), &integer_key(), ScanLayout::Row, &values),
			None
		);
		assert_eq!(
			choose_series_range(&[bound("$1")], &[], None, &integer_key(), ScanLayout::Column, &values),
			None
		);
	}
}
