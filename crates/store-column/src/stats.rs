// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::column_snapshot::ColumnStats;
use reifydb_value::{
	Result,
	value::{Value, value_type::ValueType},
};
use vortex_array::{
	VortexSessionExecute,
	aggregate_fn::{NumericalAggregateOpts, fns::min_max::min_max},
};
use vortex_session::VortexSession;

use crate::{
	error::vortex,
	scalar::to_value,
	snapshot::{ColumnBlock, ColumnChunks},
};

pub fn has_ordering(ty: &ValueType) -> bool {
	match ty {
		ValueType::Option(inner) => has_ordering(inner),
		ValueType::Any | ValueType::List(_) | ValueType::Record(_) | ValueType::Tuple(_) => false,
		_ => true,
	}
}

pub fn block_stats(block: &ColumnBlock, session: &VortexSession) -> Result<Vec<ColumnStats>> {
	let mut stats = Vec::with_capacity(block.schema.len());
	for ((name, ty, _), chunks) in block.schema.iter().zip(&block.columns) {
		let (min, max) = if has_ordering(ty) {
			fold_min_max(name, chunks, session)?
		} else {
			(None, None)
		};
		stats.push(ColumnStats {
			column: name.clone(),
			min,
			max,
			none_count: none_count(chunks, session)? as u64,
		});
	}
	Ok(stats)
}

fn fold_min_max(name: &str, chunks: &ColumnChunks, session: &VortexSession) -> Result<(Option<Value>, Option<Value>)> {
	let mut ctx = session.create_execution_ctx();
	let mut min: Option<Value> = None;
	let mut max: Option<Value> = None;
	for chunk in &chunks.chunks {
		let Some(bounds) =
			min_max(chunk, &mut ctx, NumericalAggregateOpts::default()).map_err(vortex("block_stats"))?
		else {
			continue;
		};
		let chunk_min = to_value(session, name, chunks, bounds.min)?;
		let chunk_max = to_value(session, name, chunks, bounds.max)?;
		if min.as_ref().is_none_or(|current| chunk_min < *current) {
			min = Some(chunk_min);
		}
		if max.as_ref().is_none_or(|current| chunk_max > *current) {
			max = Some(chunk_max);
		}
	}
	Ok((min, max))
}

fn none_count(chunks: &ColumnChunks, session: &VortexSession) -> Result<usize> {
	let mut ctx = session.create_execution_ctx();
	let mut total = 0;
	for chunk in &chunks.chunks {
		total += chunk.invalid_count(&mut ctx).map_err(vortex("block_stats"))?;
	}
	Ok(total)
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::ArrayRef as ArrowArrayRef;
	use arrow_schema::FieldRef;
	use reifydb_core::value::column::factory;
	use reifydb_value::value::{
		constraint::{precision::Precision, scale::Scale},
		datetime::DateTime,
		decimal::Decimal,
		value_type::{ValueType, field::from_field},
	};

	use super::*;
	use crate::{convert::to_vortex, session::new_session};

	fn chunks(ty: ValueType, nullable: bool, parts: Vec<(FieldRef, ArrowArrayRef)>) -> ColumnChunks {
		let session = new_session();
		let field_type = from_field(&parts[0].0).unwrap();
		let arrays = parts.iter().map(|p| to_vortex(&session, p).unwrap()).collect();
		ColumnChunks::new(ty, nullable, field_type, arrays)
	}

	fn empty(ty: ValueType) -> ColumnChunks {
		ColumnChunks::new(ty.clone(), false, ty.into(), vec![])
	}

	fn block(columns: Vec<(&str, ValueType, ColumnChunks)>) -> ColumnBlock {
		let schema = Arc::new(
			columns.iter().map(|(name, ty, ch)| (name.to_string(), ty.clone(), ch.nullable)).collect(),
		);
		ColumnBlock::new(schema, columns.into_iter().map(|(_, _, ch)| ch).collect())
	}

	fn stats_of(block: &ColumnBlock) -> Vec<ColumnStats> {
		block_stats(block, &new_session()).unwrap()
	}

	fn int4_chunks(parts: &[&[i32]]) -> ColumnChunks {
		chunks(ValueType::Int4, false, parts.iter().map(|p| factory::int4("c", p.to_vec())).collect())
	}

	#[test]
	fn a_single_chunk_yields_its_own_min_and_max() {
		let t = block(vec![("id", ValueType::Int4, int4_chunks(&[&[7, 2, 9, 4]]))]);
		let stats = stats_of(&t);
		assert_eq!(stats.len(), 1);
		assert_eq!(stats[0].column, "id");
		assert_eq!(stats[0].min, Some(Value::Int4(2)));
		assert_eq!(stats[0].max, Some(Value::Int4(9)));
		assert_eq!(stats[0].none_count, 0);
	}

	#[test]
	fn min_and_max_fold_across_every_chunk() {
		// The global max sits in chunk 0 and the global min in chunk 2, so an implementation
		// that reads chunks[0] alone reports max 50 / min 40 and prunes away blocks that
		// really do hold matching rows. This is the defect the whole fold exists to prevent.
		let t = block(vec![("id", ValueType::Int4, int4_chunks(&[&[40, 50], &[41, 42], &[3, 44]]))]);
		let stats = stats_of(&t);
		assert_eq!(stats[0].min, Some(Value::Int4(3)), "min lives in the last chunk");
		assert_eq!(stats[0].max, Some(Value::Int4(50)), "max lives in the first chunk");
	}

	#[test]
	fn a_nullable_column_counts_its_nones_across_chunks() {
		let chunks = chunks(
			ValueType::Int4,
			true,
			vec![
				factory::int4_optional("id", vec![Some(1), None, Some(3)]),
				factory::int4_optional("id", vec![None, Some(8)]),
			],
		);
		let t = block(vec![("id", ValueType::Int4, chunks)]);
		let stats = stats_of(&t);
		assert_eq!(stats[0].none_count, 2, "nones are counted, not rows");
		assert_eq!(stats[0].min, Some(Value::Int4(1)), "a none must not drag the min down");
		assert_eq!(stats[0].max, Some(Value::Int4(8)), "a none must not drag the max up");
	}

	#[test]
	fn an_all_none_column_has_no_min_or_max() {
		// Absent stats mean keep the block, which is the conservative direction; a bogus
		// min/max would prune live rows away.
		let chunks = chunks(ValueType::Int4, true, vec![factory::int4_optional("id", vec![None, None, None])]);
		let t = block(vec![("id", ValueType::Int4, chunks)]);
		let stats = stats_of(&t);
		assert_eq!(stats[0].min, None);
		assert_eq!(stats[0].max, None);
		assert_eq!(stats[0].none_count, 3, "none_count equals the column length");
	}

	#[test]
	fn an_empty_block_still_yields_one_stat_per_column() {
		// A pruner reading zero stats for a column cannot tell "no rows" from "not computed",
		// so an empty block must produce stats rather than an empty vector or an error.
		let t = block(vec![
			("id", ValueType::Int4, empty(ValueType::Int4)),
			("name", ValueType::Utf8, empty(ValueType::Utf8)),
		]);
		let stats = stats_of(&t);
		assert_eq!(stats.len(), 2, "one stat per schema column, never an empty vector");
		for stat in &stats {
			assert_eq!(stat.min, None);
			assert_eq!(stat.max, None);
			assert_eq!(stat.none_count, 0);
		}
	}

	#[test]
	fn stats_follow_schema_order() {
		// The pruner pairs stats with schema positions, so a reordering silently applies one
		// column's bounds to another.
		let t = block(vec![
			("c", ValueType::Int4, int4_chunks(&[&[3]])),
			("a", ValueType::Int4, int4_chunks(&[&[1]])),
			("b", ValueType::Int4, int4_chunks(&[&[2]])),
		]);
		let stats = stats_of(&t);
		let names: Vec<&str> = stats.iter().map(|s| s.column.as_str()).collect();
		assert_eq!(names, vec!["c", "a", "b"]);
		assert_eq!(stats[0].min, Some(Value::Int4(3)));
		assert_eq!(stats[1].min, Some(Value::Int4(1)));
		assert_eq!(stats[2].min, Some(Value::Int4(2)));
	}

	#[test]
	fn min_and_max_carry_the_columns_own_type() {
		// A stat widened to Int8 would compare unequal against an Int4 predicate literal and
		// the pruner would silently stop matching.
		let t = block(vec![("id", ValueType::Int4, int4_chunks(&[&[5, 6]]))]);
		let stats = stats_of(&t);
		assert!(matches!(stats[0].min, Some(Value::Int4(_))));
		assert!(matches!(stats[0].max, Some(Value::Int4(_))));
	}

	#[test]
	fn a_temporal_column_gets_min_and_max() {
		let chunks = chunks(
			ValueType::DateTime,
			false,
			vec![
				factory::datetime("ts", vec![DateTime::from_nanos(300), DateTime::from_nanos(100)]),
				factory::datetime("ts", vec![DateTime::from_nanos(900)]),
			],
		);
		let t = block(vec![("ts", ValueType::DateTime, chunks)]);
		let stats = stats_of(&t);
		assert_eq!(stats[0].min, Some(Value::DateTime(DateTime::from_nanos(100))));
		assert_eq!(stats[0].max, Some(Value::DateTime(DateTime::from_nanos(900))));
	}

	#[test]
	fn a_utf8_column_gets_min_and_max() {
		let chunks =
			chunks(ValueType::Utf8, false, vec![factory::utf8("name", vec!["pear", "apple", "quince"])]);
		let t = block(vec![("name", ValueType::Utf8, chunks)]);
		let stats = stats_of(&t);
		assert_eq!(stats[0].min, Some(Value::Utf8("apple".to_string())));
		assert_eq!(stats[0].max, Some(Value::Utf8("quince".to_string())));
	}

	#[test]
	fn a_boolean_column_gets_min_and_max() {
		let chunks = chunks(ValueType::Boolean, false, vec![factory::bool("flag", vec![true, false])]);
		let t = block(vec![("flag", ValueType::Boolean, chunks)]);
		let stats = stats_of(&t);
		assert_eq!(stats[0].min, Some(Value::Boolean(false)));
		assert_eq!(stats[0].max, Some(Value::Boolean(true)));
	}

	#[test]
	fn a_float_column_gets_min_and_max() {
		// Floats carry a total order through OrderedF64, so min_max answers rather than
		// refusing the way it used to.
		let chunks =
			chunks(ValueType::Float8, false, vec![factory::float8("v", vec![2.5f64, -1.5f64, 9.0f64])]);
		let t = block(vec![("v", ValueType::Float8, chunks)]);
		let stats = stats_of(&t);
		assert_eq!(stats[0].min, Some(Value::float8(-1.5)));
		assert_eq!(stats[0].max, Some(Value::float8(9.0)));
	}

	#[test]
	fn a_column_with_no_ordering_gets_none_bounds_but_a_real_none_count() {
		assert!(!has_ordering(&ValueType::Any));
		assert!(!has_ordering(&ValueType::List(Box::new(ValueType::Int4))));
		assert!(has_ordering(&ValueType::Option(Box::new(ValueType::DateTime))));
		assert!(has_ordering(&ValueType::Utf8));
	}

	#[test]
	fn wide_int_columns_report_no_bounds_so_the_block_is_kept() {
		// A made-up bound on a byte-list column would let the pruner drop blocks holding matching rows.
		let t = block(vec![
			(
				"u",
				ValueType::Uint16,
				chunks(
					ValueType::Uint16,
					false,
					vec![factory::uint16("u", [u128::MAX, (1u128 << 64) + 1, 1u128 << 64])],
				),
			),
			(
				"i",
				ValueType::Int16,
				chunks(
					ValueType::Int16,
					false,
					vec![factory::int16("i", [0i128, i128::MAX, -1, i128::MIN, 1])],
				),
			),
		]);
		let stats = stats_of(&t);
		for stat in &stats {
			assert_eq!(stat.min, None, "column {} must not report a min", stat.column);
			assert_eq!(stat.max, None, "column {} must not report a max", stat.column);
			assert_eq!(stat.none_count, 0);
		}
	}

	#[test]
	fn decimal_bounds_skip_nones_and_keep_the_column_scale() {
		// The bounds must come back at the column scale, and a none row must never win as zero.
		let d = |t: &str| Decimal::parse(t).unwrap();
		let column = factory::decimal_with_bitvec(
			"c",
			Precision::new(10),
			Scale::new(2),
			[d("1.50"), d("0.00"), d("-2.25"), d("0.00"), d("3.10")],
			vec![true, false, true, false, true],
		);
		let ty = ValueType::Decimal {
			precision: Precision::new(10),
			scale: Scale::new(2),
		};
		let t = block(vec![("c", ty.clone(), chunks(ty, true, vec![column]))]);
		let stats = stats_of(&t);
		let min = stats[0].min.clone().expect("decimal column has a min");
		let max = stats[0].max.clone().expect("decimal column has a max");
		assert_eq!(min.to_string(), "-2.25");
		assert_eq!(max.to_string(), "3.10");
		assert!(matches!(&min, Value::Decimal(v) if v.scale() == 2));
		assert_eq!(stats[0].none_count, 2);
	}
}
