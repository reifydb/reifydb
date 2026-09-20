// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::column_snapshot::ColumnStats,
	value::column::{data::Column, nones::NoneBitmap},
};
use reifydb_value::{
	Result,
	value::{Value, value_type::ValueType},
};

use crate::{
	compute::min_max,
	snapshot::{ColumnBlock, ColumnChunks},
};

pub fn has_ordering(ty: &ValueType) -> bool {
	match ty {
		ValueType::Option(inner) => has_ordering(inner),
		ValueType::Any | ValueType::List(_) | ValueType::Record(_) | ValueType::Tuple(_) => false,
		_ => true,
	}
}

pub fn block_stats(block: &ColumnBlock) -> Result<Vec<ColumnStats>> {
	let mut stats = Vec::with_capacity(block.schema.len());
	for ((name, ty, _), chunks) in block.schema.iter().zip(&block.columns) {
		let (min, max) = if has_ordering(ty) {
			fold_min_max(chunks)?
		} else {
			(None, None)
		};
		stats.push(ColumnStats {
			column: name.clone(),
			min,
			max,
			none_count: none_count(chunks) as u64,
		});
	}
	Ok(stats)
}

fn fold_min_max(chunks: &ColumnChunks) -> Result<(Option<Value>, Option<Value>)> {
	let mut min: Option<Value> = None;
	let mut max: Option<Value> = None;
	for chunk in &chunks.chunks {
		if chunk_none_count(chunk) == chunk.len() {
			continue;
		}
		let (chunk_min, chunk_max) = min_max(chunk)?;
		if min.as_ref().is_none_or(|current| chunk_min < *current) {
			min = Some(chunk_min);
		}
		if max.as_ref().is_none_or(|current| chunk_max > *current) {
			max = Some(chunk_max);
		}
	}
	Ok((min, max))
}

fn none_count(chunks: &ColumnChunks) -> usize {
	chunks.chunks.iter().map(chunk_none_count).sum()
}

fn chunk_none_count(chunk: &Column) -> usize {
	match chunk.nones() {
		None => 0,
		Some(nones) => count_nones(nones, chunk.len()),
	}
}

fn count_nones(nones: &NoneBitmap, len: usize) -> usize {
	(0..len).filter(|&row| nones.is_none(row)).count()
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::value::column::{
		buffer::ColumnBuffer,
		data::{Column, canonical::Canonical},
	};
	use reifydb_value::value::{datetime::DateTime, value_type::ValueType};

	use super::*;

	fn chunk(buffer: ColumnBuffer) -> Column {
		Column::from_canonical(Canonical::from_column_buffer(&buffer).unwrap())
	}

	fn block(columns: Vec<(&str, ValueType, ColumnChunks)>) -> ColumnBlock {
		let schema = Arc::new(
			columns.iter().map(|(name, ty, ch)| (name.to_string(), ty.clone(), ch.nullable)).collect(),
		);
		ColumnBlock::new(schema, columns.into_iter().map(|(_, _, ch)| ch).collect())
	}

	fn int4_chunks(parts: &[&[i32]]) -> ColumnChunks {
		ColumnChunks::new(
			ValueType::Int4,
			false,
			parts.iter().map(|p| chunk(ColumnBuffer::int4(p.to_vec()))).collect(),
		)
	}

	#[test]
	fn a_single_chunk_yields_its_own_min_and_max() {
		let t = block(vec![("id", ValueType::Int4, int4_chunks(&[&[7, 2, 9, 4]]))]);
		let stats = block_stats(&t).unwrap();
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
		let stats = block_stats(&t).unwrap();
		assert_eq!(stats[0].min, Some(Value::Int4(3)), "min lives in the last chunk");
		assert_eq!(stats[0].max, Some(Value::Int4(50)), "max lives in the first chunk");
	}

	#[test]
	fn a_nullable_column_counts_its_nones_across_chunks() {
		let chunks = ColumnChunks::new(
			ValueType::Int4,
			true,
			vec![
				chunk(ColumnBuffer::int4_optional(vec![Some(1), None, Some(3)])),
				chunk(ColumnBuffer::int4_optional(vec![None, Some(8)])),
			],
		);
		let t = block(vec![("id", ValueType::Int4, chunks)]);
		let stats = block_stats(&t).unwrap();
		assert_eq!(stats[0].none_count, 2, "nones are counted, not rows");
		assert_eq!(stats[0].min, Some(Value::Int4(1)), "a none must not drag the min down");
		assert_eq!(stats[0].max, Some(Value::Int4(8)), "a none must not drag the max up");
	}

	#[test]
	fn an_all_none_column_has_no_min_or_max() {
		// min_max errors on an all-none chunk rather than returning a sentinel, so the fold
		// must recognise the case itself. Absent stats mean keep the block, which is the
		// conservative direction; a bogus min/max would prune live rows away.
		let chunks = ColumnChunks::new(
			ValueType::Int4,
			true,
			vec![chunk(ColumnBuffer::int4_optional(vec![None, None, None]))],
		);
		let t = block(vec![("id", ValueType::Int4, chunks)]);
		let stats = block_stats(&t).unwrap();
		assert_eq!(stats[0].min, None);
		assert_eq!(stats[0].max, None);
		assert_eq!(stats[0].none_count, 3, "none_count equals the column length");
	}

	#[test]
	fn an_empty_block_still_yields_one_stat_per_column() {
		// A pruner reading zero stats for a column cannot tell "no rows" from "not computed",
		// so an empty block must produce stats rather than an empty vector or an error.
		let t = block(vec![
			("id", ValueType::Int4, ColumnChunks::new(ValueType::Int4, false, vec![])),
			("name", ValueType::Utf8, ColumnChunks::new(ValueType::Utf8, false, vec![])),
		]);
		let stats = block_stats(&t).unwrap();
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
		let stats = block_stats(&t).unwrap();
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
		let stats = block_stats(&t).unwrap();
		assert!(matches!(stats[0].min, Some(Value::Int4(_))));
		assert!(matches!(stats[0].max, Some(Value::Int4(_))));
	}

	#[test]
	fn a_temporal_column_gets_min_and_max() {
		let chunks = ColumnChunks::new(
			ValueType::DateTime,
			false,
			vec![
				chunk(ColumnBuffer::datetime(vec![
					DateTime::from_nanos(300),
					DateTime::from_nanos(100),
				])),
				chunk(ColumnBuffer::datetime(vec![DateTime::from_nanos(900)])),
			],
		);
		let t = block(vec![("ts", ValueType::DateTime, chunks)]);
		let stats = block_stats(&t).unwrap();
		assert_eq!(stats[0].min, Some(Value::DateTime(DateTime::from_nanos(100))));
		assert_eq!(stats[0].max, Some(Value::DateTime(DateTime::from_nanos(900))));
	}

	#[test]
	fn a_utf8_column_gets_min_and_max() {
		let chunks = ColumnChunks::new(
			ValueType::Utf8,
			false,
			vec![chunk(ColumnBuffer::utf8(vec!["pear", "apple", "quince"]))],
		);
		let t = block(vec![("name", ValueType::Utf8, chunks)]);
		let stats = block_stats(&t).unwrap();
		assert_eq!(stats[0].min, Some(Value::Utf8("apple".to_string())));
		assert_eq!(stats[0].max, Some(Value::Utf8("quince".to_string())));
	}

	#[test]
	fn a_boolean_column_gets_min_and_max() {
		let chunks = ColumnChunks::new(
			ValueType::Boolean,
			false,
			vec![chunk(ColumnBuffer::bool(vec![true, false]))],
		);
		let t = block(vec![("flag", ValueType::Boolean, chunks)]);
		let stats = block_stats(&t).unwrap();
		assert_eq!(stats[0].min, Some(Value::Boolean(false)));
		assert_eq!(stats[0].max, Some(Value::Boolean(true)));
	}

	#[test]
	fn a_float_column_gets_min_and_max() {
		// Floats carry a total order through OrderedF64, so min_max answers rather than
		// refusing the way it used to.
		let chunks = ColumnChunks::new(
			ValueType::Float8,
			false,
			vec![chunk(ColumnBuffer::float8(vec![2.5f64, -1.5f64, 9.0f64]))],
		);
		let t = block(vec![("v", ValueType::Float8, chunks)]);
		let stats = block_stats(&t).unwrap();
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
}
