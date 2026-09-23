// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_column::predicate::{ColRef, Predicate};
use reifydb_core::interface::catalog::series::Series;
use reifydb_value::value::{Value, value_type::ValueType};

pub(crate) fn series_scan_predicate(
	series: &Series,
	key_range_start: Option<u64>,
	key_range_end: Option<u64>,
	variant_tag: Option<u8>,
) -> Option<Predicate> {
	let key = ColRef::from(series.key.column());
	let mut clauses = Vec::new();
	if let Some(value) = key_range_start.and_then(|raw| key_value(series, raw)) {
		clauses.push(Predicate::GtEq(key.clone(), value));
	}
	if let Some(value) = key_range_end.and_then(|raw| key_value(series, raw)) {
		clauses.push(Predicate::Lt(key, value));
	}
	if let Some(tag) = variant_tag {
		clauses.push(Predicate::Eq(ColRef::from("tag"), Value::Uint1(tag)));
	}
	match clauses.len() {
		0 => None,
		1 => clauses.pop(),
		_ => Some(Predicate::And(clauses)),
	}
}

fn key_value(series: &Series, raw: u64) -> Option<Value> {
	let expected = series.key_column_type().unwrap_or(ValueType::Uint8);
	let value = series.key_from_u64(raw);
	(value.get_type() == expected).then_some(value)
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::TimeSource,
		interface::catalog::{
			column::{Column, ColumnIndex},
			id::{ColumnId, NamespaceId, SeriesId},
			series::SeriesKey,
		},
	};
	use reifydb_value::value::{constraint::TypeConstraint, sumtype::SumTypeId};

	use super::*;

	fn series_with(key: SeriesKey, columns: Vec<Column>, tag: Option<SumTypeId>) -> Series {
		Series {
			id: SeriesId(1),
			namespace: NamespaceId(1),
			name: "s".into(),
			columns,
			tag,
			key,
			primary_key: None,
			partition_by: vec![],
			time: TimeSource::Processing,
		}
	}

	fn column(name: &str, ty: ValueType) -> Column {
		Column {
			id: ColumnId(1),
			name: name.into(),
			constraint: TypeConstraint::unconstrained(ty),
			properties: vec![],
			index: ColumnIndex(0),
			auto_increment: false,
			dictionary_id: None,
		}
	}

	fn integer_key() -> SeriesKey {
		SeriesKey::Integer {
			column: "k".into(),
		}
	}

	#[test]
	fn no_filter_fields_produce_no_predicate() {
		// An unfiltered scan must hand the reader nothing rather than an empty And. An empty
		// And evaluates to all rows set, so it costs a mask per batch to say what None says free.
		let series = series_with(integer_key(), vec![], None);
		assert!(series_scan_predicate(&series, None, None, None).is_none());
	}

	#[test]
	fn a_lone_start_bound_is_not_wrapped_in_and() {
		// A single clause stands alone. Wrapping it adds a mask combine per batch for nothing.
		let series = series_with(integer_key(), vec![], None);
		let predicate = series_scan_predicate(&series, Some(10), None, None).unwrap();
		match predicate {
			Predicate::GtEq(column, value) => {
				assert_eq!(column, ColRef::from("k"));
				assert_eq!(value, Value::Uint8(10));
			}
			other => panic!("expected a bare GtEq, got {other:?}"),
		}
	}

	#[test]
	fn both_bounds_combine_into_one_and() {
		// Two bounds are a conjunction, not two predicates and not a nested tree.
		let series = series_with(integer_key(), vec![], None);
		let predicate = series_scan_predicate(&series, Some(10), Some(20), None).unwrap();
		match predicate {
			Predicate::And(clauses) => {
				assert_eq!(clauses.len(), 2);
				assert!(matches!(clauses[0], Predicate::GtEq(..)));
				assert!(matches!(clauses[1], Predicate::Lt(..)));
			}
			other => panic!("expected an And of two clauses, got {other:?}"),
		}
	}

	#[test]
	fn the_end_bound_is_exclusive() {
		// Bucket ranges are half open, so the end bound must be Lt. LtEq pulls in the first row
		// of the next bucket, which the row path would never have returned.
		let series = series_with(integer_key(), vec![], None);
		let predicate = series_scan_predicate(&series, None, Some(20), None).unwrap();
		match predicate {
			Predicate::Lt(_, value) => assert_eq!(value, Value::Uint8(20)),
			other => panic!("expected Lt, got {other:?}"),
		}
	}

	#[test]
	fn the_tag_clause_names_the_block_column() {
		// The block schema calls the discriminant column "tag" regardless of the declared sum
		// type name. Naming it anything else finds no column and the clause matches nothing.
		let series = series_with(integer_key(), vec![], Some(SumTypeId(7)));
		let predicate = series_scan_predicate(&series, None, None, Some(3)).unwrap();
		match predicate {
			Predicate::Eq(column, value) => {
				assert_eq!(column, ColRef::from("tag"));
				assert_eq!(value, Value::Uint1(3));
			}
			other => panic!("expected Eq on tag, got {other:?}"),
		}
	}

	#[test]
	fn the_key_clause_carries_the_declared_key_type() {
		// The block stores the key in its declared type. A Uint8 bound against an Int4 column
		// compares across types and matches nothing, dropping every row of the scan.
		let series = series_with(integer_key(), vec![column("k", ValueType::Int4)], None);
		let predicate = series_scan_predicate(&series, Some(10), None, None).unwrap();
		match predicate {
			Predicate::GtEq(_, value) => assert_eq!(value, Value::Int4(10)),
			other => panic!("expected GtEq carrying an Int4, got {other:?}"),
		}
	}

	#[test]
	fn an_undeclared_key_column_still_filters_as_uint8() {
		// With no declared column both the block schema and the conversion fall back to Uint8,
		// so the two agree and the clause is safe to emit. Dropping it here loses real pruning.
		let series = series_with(integer_key(), vec![column("other", ValueType::Utf8)], None);
		let predicate = series_scan_predicate(&series, Some(10), None, None).unwrap();
		match predicate {
			Predicate::GtEq(_, value) => assert_eq!(value, Value::Uint8(10)),
			other => panic!("expected GtEq carrying a Uint8, got {other:?}"),
		}
	}

	#[test]
	fn an_unconvertible_key_type_drops_the_key_clause_but_keeps_the_tag() {
		// A key type the conversion has no arm for silently yields a Uint8 that cannot match a
		// Utf8 column. Emitting it returns nothing at all, so the clause is dropped and the
		// residual filter above the scan does the work. Unrelated clauses must survive.
		let series = series_with(integer_key(), vec![column("k", ValueType::Utf8)], Some(SumTypeId(7)));
		let predicate = series_scan_predicate(&series, Some(10), Some(20), Some(3)).unwrap();
		match predicate {
			Predicate::Eq(column, value) => {
				assert_eq!(column, ColRef::from("tag"));
				assert_eq!(value, Value::Uint1(3));
			}
			other => panic!("expected only the tag clause to survive, got {other:?}"),
		}
	}

	#[test]
	fn an_unconvertible_key_type_with_no_tag_produces_no_predicate() {
		// Dropping every clause must leave None, not an And of nothing. An empty And would be
		// harmless here but a one-clause And would not, so the arity rules are pinned together.
		let series = series_with(integer_key(), vec![column("k", ValueType::Utf8)], None);
		assert!(series_scan_predicate(&series, Some(10), Some(20), None).is_none());
	}
}
