// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem::discriminant;

use reifydb_column::predicate::{ColRef, Predicate};
use reifydb_core::interface::catalog::column_snapshot::{ColumnSnapshot, ColumnSnapshotSource};
use reifydb_value::value::Value;

pub(crate) fn prune_series_snapshots(
	snapshots: Vec<ColumnSnapshot>,
	key_range_start: Option<u64>,
	key_range_end: Option<u64>,
	predicate: Option<&Predicate>,
) -> Vec<ColumnSnapshot> {
	snapshots
		.into_iter()
		.filter(|snapshot| overlaps_key_range(snapshot, key_range_start, key_range_end))
		.filter(|snapshot| predicate.is_none_or(|predicate| stats_admit(snapshot, predicate)))
		.collect()
}

fn overlaps_key_range(snapshot: &ColumnSnapshot, start: Option<u64>, end: Option<u64>) -> bool {
	let ColumnSnapshotSource::SeriesBucket {
		bucket_start,
		bucket_width,
		..
	} = snapshot.source
	else {
		return true;
	};
	let bucket_end = bucket_start.saturating_add(bucket_width);
	if start.is_some_and(|start| bucket_end <= start) {
		return false;
	}
	if end.is_some_and(|end| bucket_start >= end) {
		return false;
	}
	true
}

fn stats_admit(snapshot: &ColumnSnapshot, predicate: &Predicate) -> bool {
	match predicate {
		Predicate::Eq(column, value) => match bounds(snapshot, column, value) {
			Some((min, max)) => value >= min && value <= max,
			None => true,
		},
		Predicate::Lt(column, value) => match bounds(snapshot, column, value) {
			Some((min, _)) => min < value,
			None => true,
		},
		Predicate::LtEq(column, value) => match bounds(snapshot, column, value) {
			Some((min, _)) => min <= value,
			None => true,
		},
		Predicate::Gt(column, value) => match bounds(snapshot, column, value) {
			Some((_, max)) => max > value,
			None => true,
		},
		Predicate::GtEq(column, value) => match bounds(snapshot, column, value) {
			Some((_, max)) => max >= value,
			None => true,
		},
		Predicate::And(clauses) => clauses.iter().all(|clause| stats_admit(snapshot, clause)),
		Predicate::Or(clauses) => clauses.is_empty() || clauses.iter().any(|clause| stats_admit(snapshot, clause)),
		_ => true,
	}
}

fn bounds<'a>(snapshot: &'a ColumnSnapshot, column: &ColRef, probe: &Value) -> Option<(&'a Value, &'a Value)> {
	let stats = snapshot.stats.iter().find(|stats| stats.column == column.0)?;
	let min = stats.min.as_ref()?;
	let max = stats.max.as_ref()?;
	if !comparable(min, probe) || !comparable(max, probe) {
		return None;
	}
	Some((min, max))
}

fn comparable(bound: &Value, probe: &Value) -> bool {
	if matches!(
		bound,
		Value::None {
			..
		} | Value::Any(_) | Value::List(_) | Value::Record(_) | Value::Tuple(_)
	) {
		return false;
	}
	discriminant(bound) == discriminant(probe)
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{
			column_snapshot::ColumnStats,
			id::{ColumnSnapshotId, NamespaceId, SeriesId},
		},
	};

	use super::*;

	fn bucket(start: u64, width: u64, stats: Vec<ColumnStats>) -> ColumnSnapshot {
		ColumnSnapshot {
			id: ColumnSnapshotId(start),
			namespace: NamespaceId(1),
			source: ColumnSnapshotSource::SeriesBucket {
				series_id: SeriesId(1),
				bucket_start: start,
				bucket_width: width,
				partition: None,
				sequence_counter: 0,
				sealed_at_commit_version: CommitVersion(1),
			},
			row_count: 10,
			partition_values: Vec::new(),
			stats,
		}
	}

	fn stat(column: &str, min: Option<i32>, max: Option<i32>) -> ColumnStats {
		ColumnStats {
			column: column.into(),
			min: min.map(Value::Int4),
			max: max.map(Value::Int4),
			none_count: 0,
		}
	}

	fn starts(snapshots: &[ColumnSnapshot]) -> Vec<u64> {
		snapshots.iter()
			.map(|snapshot| match snapshot.source {
				ColumnSnapshotSource::SeriesBucket {
					bucket_start,
					..
				} => bucket_start,
				_ => panic!("fixture builds series buckets only"),
			})
			.collect()
	}

	fn three_buckets() -> Vec<ColumnSnapshot> {
		vec![bucket(0, 100, vec![]), bucket(100, 100, vec![]), bucket(200, 100, vec![])]
	}

	#[test]
	fn no_filters_keeps_everything() {
		// The identity case. A scan with no bounds and no predicate must read every sealed
		// bucket, or a bare `from series` silently returns a subset of the data.
		let kept = prune_series_snapshots(three_buckets(), None, None, None);
		assert_eq!(starts(&kept), vec![0, 100, 200]);
	}

	#[test]
	fn a_range_below_every_bucket_keeps_nothing() {
		// Pruning has to be able to reach zero. If it always leaves one block behind, a query
		// for a key window that predates the series still pays for a block read.
		let kept = prune_series_snapshots(three_buckets(), None, Some(0), None);
		assert!(kept.is_empty());
	}

	#[test]
	fn a_range_inside_one_bucket_keeps_one() {
		// The whole point of the layer: a narrow window touches exactly the bucket that holds it.
		let kept = prune_series_snapshots(three_buckets(), Some(120), Some(130), None);
		assert_eq!(starts(&kept), vec![100]);
	}

	#[test]
	fn a_range_that_straddles_a_boundary_keeps_both() {
		// Dropping a block the query needed is a silent wrong answer, so a window crossing a
		// boundary must keep both sides. Trimming the surplus rows is the predicate's job.
		let kept = prune_series_snapshots(three_buckets(), Some(99), Some(101), None);
		assert_eq!(starts(&kept), vec![0, 100]);
	}

	#[test]
	fn a_bucket_that_ends_exactly_at_the_range_start_is_dropped() {
		// Bucket bounds are half open: bucket 0 covers 0..100, so key 100 is not in it. An
		// off-by-one here keeps a block that can never contribute a row.
		let kept = prune_series_snapshots(three_buckets(), Some(100), None, None);
		assert_eq!(starts(&kept), vec![100, 200]);
	}

	#[test]
	fn a_bucket_that_starts_exactly_at_the_range_end_is_dropped() {
		// The other end of the same rule, and the dangerous one: the range end is exclusive, so
		// a bucket starting at it holds nothing the query asked for.
		let kept = prune_series_snapshots(three_buckets(), None, Some(100), None);
		assert_eq!(starts(&kept), vec![0]);
	}

	#[test]
	fn a_missing_stats_entry_keeps_the_block() {
		// A block materialized before a column existed, or by an older writer, carries no entry
		// for it. Absence of evidence is not evidence of absence: keep the block.
		let snapshots = vec![bucket(0, 100, vec![stat("other", Some(0), Some(1))])];
		let predicate = Predicate::Eq(ColRef::from("value"), Value::Int4(500));
		let kept = prune_series_snapshots(snapshots, None, None, Some(&predicate));
		assert_eq!(starts(&kept), vec![0]);
	}

	#[test]
	fn a_none_min_keeps_the_block() {
		// An all-none column has no min to compare against. Treating a missing bound as an
		// empty range would drop every block whose column was not fully populated.
		let snapshots = vec![bucket(0, 100, vec![stat("value", None, Some(10))])];
		let predicate = Predicate::Eq(ColRef::from("value"), Value::Int4(500));
		let kept = prune_series_snapshots(snapshots, None, None, Some(&predicate));
		assert_eq!(starts(&kept), vec![0]);
	}

	#[test]
	fn stats_that_exclude_the_value_drop_the_block() {
		// The actual win: the equality target sits outside one block's range entirely, so that
		// block is skipped without ever being opened.
		let snapshots = vec![
			bucket(0, 100, vec![stat("value", Some(0), Some(9))]),
			bucket(100, 100, vec![stat("value", Some(10), Some(19))]),
		];
		let predicate = Predicate::Eq(ColRef::from("value"), Value::Int4(15));
		let kept = prune_series_snapshots(snapshots, None, None, Some(&predicate));
		assert_eq!(starts(&kept), vec![100]);
	}

	#[test]
	fn a_value_on_the_bound_keeps_the_block() {
		// min and max are inclusive. An exclusive read of either bound drops the one block that
		// holds the matching row whenever the query targets an extreme.
		let snapshots = vec![bucket(0, 100, vec![stat("value", Some(10), Some(20))])];
		for probe in [10, 20] {
			let predicate = Predicate::Eq(ColRef::from("value"), Value::Int4(probe));
			let kept = prune_series_snapshots(vec![snapshots[0].clone()], None, None, Some(&predicate));
			assert_eq!(starts(&kept), vec![0], "value {probe} sits on a bound and must be kept");
		}
	}

	#[test]
	fn a_type_mismatch_between_bound_and_probe_keeps_the_block() {
		// Comparing values of different types panics rather than returning an ordering, so the
		// bound must be type checked before use. Without the guard this test aborts the process.
		let snapshots = vec![bucket(0, 100, vec![stat("value", Some(0), Some(9))])];
		let predicate = Predicate::Eq(ColRef::from("value"), Value::Utf8("x".into()));
		let kept = prune_series_snapshots(snapshots, None, None, Some(&predicate));
		assert_eq!(starts(&kept), vec![0]);
	}

	#[test]
	fn a_non_orderable_bound_keeps_the_block() {
		// Statistics for a list or record column should never be written, but a stale or
		// corrupt row must not reach the comparison: ordering those variants panics.
		let stats = vec![ColumnStats {
			column: "value".into(),
			min: Some(Value::List(vec![])),
			max: Some(Value::List(vec![])),
			none_count: 0,
		}];
		let snapshots = vec![bucket(0, 100, stats)];
		let predicate = Predicate::Eq(ColRef::from("value"), Value::List(vec![]));
		let kept = prune_series_snapshots(snapshots, None, None, Some(&predicate));
		assert_eq!(starts(&kept), vec![0]);
	}

	#[test]
	fn each_comparison_drops_only_the_blocks_it_excludes() {
		// One block per side of the probe pins the direction of every operator. A flipped
		// comparison still prunes something, so a test on a single block would not catch it.
		let low = bucket(0, 100, vec![stat("value", Some(0), Some(10))]);
		let high = bucket(100, 100, vec![stat("value", Some(20), Some(30))]);
		let cases = [
			(Predicate::Lt(ColRef::from("value"), Value::Int4(15)), vec![0u64]),
			(Predicate::LtEq(ColRef::from("value"), Value::Int4(0)), vec![0]),
			(Predicate::Gt(ColRef::from("value"), Value::Int4(15)), vec![100]),
			(Predicate::GtEq(ColRef::from("value"), Value::Int4(30)), vec![100]),
			(Predicate::Lt(ColRef::from("value"), Value::Int4(0)), vec![]),
			(Predicate::Gt(ColRef::from("value"), Value::Int4(30)), vec![]),
		];
		for (predicate, expected) in cases {
			let kept = prune_series_snapshots(
				vec![low.clone(), high.clone()],
				None,
				None,
				Some(&predicate),
			);
			assert_eq!(starts(&kept), expected, "{predicate:?}");
		}
	}

	#[test]
	fn and_drops_a_block_that_any_clause_excludes() {
		// Every clause must hold, so one impossible clause is enough to rule the block out even
		// while the other clause matches its range.
		let snapshots = vec![bucket(0, 100, vec![stat("value", Some(0), Some(9))])];
		let predicate = Predicate::And(vec![
			Predicate::GtEq(ColRef::from("value"), Value::Int4(5)),
			Predicate::GtEq(ColRef::from("value"), Value::Int4(50)),
		]);
		let kept = prune_series_snapshots(snapshots, None, None, Some(&predicate));
		assert!(kept.is_empty());
	}

	#[test]
	fn or_keeps_a_block_that_any_clause_admits() {
		// One satisfiable branch means the block can still produce a row. Treating Or like And
		// here would drop blocks that the query matches.
		let snapshots = vec![bucket(0, 100, vec![stat("value", Some(0), Some(9))])];
		let predicate = Predicate::Or(vec![
			Predicate::GtEq(ColRef::from("value"), Value::Int4(50)),
			Predicate::Lt(ColRef::from("value"), Value::Int4(5)),
		]);
		let kept = prune_series_snapshots(snapshots, None, None, Some(&predicate));
		assert_eq!(starts(&kept), vec![0]);
		let impossible = Predicate::Or(vec![
			Predicate::GtEq(ColRef::from("value"), Value::Int4(50)),
			Predicate::Gt(ColRef::from("value"), Value::Int4(90)),
		]);
		let kept = prune_series_snapshots(
			vec![bucket(0, 100, vec![stat("value", Some(0), Some(9))])],
			None,
			None,
			Some(&impossible),
		);
		assert!(kept.is_empty());
	}

	#[test]
	fn a_predicate_the_statistics_cannot_answer_keeps_the_block() {
		// Negation and inequality say nothing about a range, and a none count is not consulted
		// here. Each must fall through to keeping the block rather than guessing.
		let snapshots = vec![bucket(0, 100, vec![stat("value", Some(0), Some(9))])];
		let cases = [
			Predicate::Ne(ColRef::from("value"), Value::Int4(5)),
			Predicate::Not(Box::new(Predicate::Eq(ColRef::from("value"), Value::Int4(5)))),
			Predicate::In(ColRef::from("value"), vec![Value::Int4(500)]),
			Predicate::IsNone(ColRef::from("value")),
			Predicate::IsNotNone(ColRef::from("value")),
		];
		for predicate in cases {
			let kept = prune_series_snapshots(
				vec![snapshots[0].clone()],
				None,
				None,
				Some(&predicate),
			);
			assert_eq!(starts(&kept), vec![0], "{predicate:?}");
		}
	}

	#[test]
	fn the_key_range_and_the_statistics_both_apply() {
		// The two filters are independent, so a block has to survive both. Running only the
		// first would leave the second's work to the block read it was meant to avoid.
		let snapshots = vec![
			bucket(0, 100, vec![stat("value", Some(0), Some(9))]),
			bucket(100, 100, vec![stat("value", Some(0), Some(9))]),
			bucket(200, 100, vec![stat("value", Some(50), Some(59))]),
		];
		let predicate = Predicate::GtEq(ColRef::from("value"), Value::Int4(50));
		let kept = prune_series_snapshots(snapshots, Some(100), None, Some(&predicate));
		assert_eq!(starts(&kept), vec![200]);
	}
}
