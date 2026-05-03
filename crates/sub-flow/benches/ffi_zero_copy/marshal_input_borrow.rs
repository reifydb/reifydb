// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#![allow(clippy::needless_range_loop)]

#[path = "common.rs"]
mod common;

use std::time::Instant;

use common::with_counting;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff, Diffs},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_sdk::ffi::arena::Arena;
use reifydb_type::{
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{
		constraint::bytes::MaxBytes,
		container::{number::NumberContainer, utf8::Utf8Container},
		datetime::DateTime,
		row_number::RowNumber,
	},
};
fn build_numeric_utf8_change(n_rows: usize) -> Change {
	let int_data: Vec<i64> = (0..n_rows as i64).collect();
	let utf8_data: Vec<String> = (0..n_rows).map(|i| format!("v{}", i)).collect();

	let int_col = ColumnBuffer::Int8(NumberContainer::from_parts(CowVec::new(int_data)));
	let utf8_col = ColumnBuffer::Utf8 {
		container: Utf8Container::from_vec(utf8_data),
		max_bytes: MaxBytes::MAX,
	};
	let cols = vec![
		ColumnWithName::new(Fragment::internal("a"), int_col),
		ColumnWithName::new(Fragment::internal("b"), utf8_col),
	];
	let row_numbers: Vec<RowNumber> = (1..=n_rows as u64).map(RowNumber).collect();
	let now = DateTime::default();
	let timestamps: Vec<DateTime> = vec![now; n_rows];
	let columns = Columns::with_system_columns(cols, row_numbers, timestamps.clone(), timestamps);

	let mut diffs: Diffs = Diffs::new();
	diffs.push(Diff::insert(columns));
	Change::from_flow(FlowNodeId(1), CommitVersion(1), diffs, now)
}

fn bench_marshal_numeric_utf8(n_rows: usize, iters: usize) {
	let change = build_numeric_utf8_change(n_rows);
	let mut arena = Arena::new();

	let _ = arena.marshal_change(&change);
	arena.clear();

	let start = Instant::now();
	let (_, counts) = with_counting(|| {
		for _ in 0..iters {
			let _ffi = arena.marshal_change(&change);
		}
		arena.clear();
	});
	let elapsed = start.elapsed();

	let allocs_per_iter = counts.allocs as f64 / iters as f64;
	let bytes_per_iter = counts.bytes_allocated as f64 / iters as f64;
	println!(
		"  rows={:<6} iters={:<6} time={:?} total_allocs={} ({:.4}/iter)  total_bytes={} ({:.1}/iter)",
		n_rows, iters, elapsed, counts.allocs, allocs_per_iter, counts.bytes_allocated, bytes_per_iter
	);

	assert!(
		allocs_per_iter < 0.05,
		"per-iter allocation rate ({:.4}) exceeded 0.05 for {} rows; input borrow may have regressed",
		allocs_per_iter,
		n_rows
	);
}

fn main() {
	println!("\n[marshal-input-borrow] verifies input zero-copy");
	for &n in &[1usize, 16, 256, 4096] {
		bench_marshal_numeric_utf8(n, 10_000);
	}
	println!("\nDone.");
}
