// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#![allow(clippy::needless_range_loop)]

#[path = "common.rs"]
mod common;

use std::{collections::HashMap, time::Instant};

use common::with_counting;
use reifydb_abi::{
	data::column::ColumnTypeCode, flow::diff::DiffType, operator::capabilities::CAPABILITY_ALL_STANDARD,
};
use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_sdk::{
	error::Result as SdkResult,
	operator::{
		FFIOperator, FFIOperatorMetadata, change::BorrowedChange, column::OperatorColumn,
		context::OperatorContext,
	},
	testing::{builders::TestChangeBuilder, harness::TestHarnessBuilder},
};
use reifydb_type::value::{Value, row_number::RowNumber};

struct EvenFilter;

impl FFIOperatorMetadata for EvenFilter {
	const NAME: &'static str = "even_filter_bench";
	const API: u32 = 1;
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "Bench fixture: keeps rows where col[0] (Int8) is even";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
}

impl FFIOperator for EvenFilter {
	fn new(_id: FlowNodeId, _config: &HashMap<String, Value>) -> SdkResult<Self> {
		Ok(Self)
	}

	fn apply(&mut self, ctx: &mut OperatorContext, input: BorrowedChange<'_>) -> SdkResult<()> {
		let mut builder = ctx.builder();
		for diff in input.diffs() {
			if diff.kind() != DiffType::Insert {
				continue;
			}
			let post = diff.post();

			let int_col = match post.columns().next() {
				Some(c) => c,
				None => continue,
			};
			let int_slice: &[i64] = match unsafe { int_col.as_slice::<i64>() } {
				Some(s) => s,
				None => continue,
			};

			let kept_n: usize = int_slice.iter().filter(|&&v| v % 2 == 0).count();
			if kept_n == 0 {
				continue;
			}

			let out_col = builder.acquire(ColumnTypeCode::Int8, kept_n)?;
			out_col.grow(kept_n)?;
			let dst = out_col.data_ptr() as *mut i64;
			let mut out_idx = 0usize;
			for &v in int_slice {
				if v % 2 == 0 {
					unsafe {
						core::ptr::write(dst.add(out_idx), v);
					}
					out_idx += 1;
				}
			}
			let committed = out_col.commit(kept_n)?;
			let row_numbers: Vec<RowNumber> = (1..=kept_n as u64).map(RowNumber).collect();
			builder.emit_insert(&[committed], &["a"], &row_numbers)?;
		}
		Ok(())
	}

	fn pull(&mut self, _ctx: &mut OperatorContext, _row_numbers: &[RowNumber]) -> SdkResult<()> {
		Ok(())
	}
}

fn bench_ffi_filter(n_rows: usize, iters: usize) {
	let mut harness =
		TestHarnessBuilder::<EvenFilter>::new().with_node_id(FlowNodeId(1)).build().expect("build harness");

	let mut tcb = TestChangeBuilder::new();
	for i in 0..n_rows {
		tcb = tcb.insert_row((i as u64) + 1, vec![Value::Int8(i as i64)]);
	}
	let input = tcb.build();

	for _ in 0..2 {
		let _ = harness.apply(input.clone()).expect("warmup apply");
	}
	harness.clear_history();

	let start = Instant::now();
	let (_, counts) = with_counting(|| {
		for _ in 0..iters {
			let _out = harness.apply(input.clone()).expect("apply");
		}
	});
	let elapsed = start.elapsed();

	let allocs_per_iter = counts.allocs as f64 / iters as f64;
	let bytes_per_iter = counts.bytes_allocated as f64 / iters as f64;
	let allocs_per_row = counts.allocs as f64 / (iters as f64 * n_rows as f64);
	let bytes_per_row = counts.bytes_allocated as f64 / (iters as f64 * n_rows as f64);
	let ns_per_call = elapsed.as_nanos() as f64 / iters as f64;
	println!(
		"  rows={:<6} iters={:<6} {:>9.0} ns/call  allocs={:.2}/iter ({:.4}/row)  bytes={:.1}/iter ({:.2}/row)",
		n_rows, iters, ns_per_call, allocs_per_iter, allocs_per_row, bytes_per_iter, bytes_per_row
	);

	let _ = (allocs_per_iter, bytes_per_iter, allocs_per_row, bytes_per_row);
}

fn main() {
	println!("\n[ffi-filter] verifies zero-copy FFI ABI: input borrow + output builders");
	for &n in &[1usize, 16, 256, 4096] {
		bench_ffi_filter(n, 1_000);
	}
	println!("\nDone.");
}
