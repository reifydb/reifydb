// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#![allow(clippy::needless_range_loop)]

#[path = "common.rs"]
mod common;

use std::{cell::RefCell, collections::HashMap, rc::Rc, time::Instant};

use common::with_counting;
use reifydb_abi::{flow::diff::DiffType, operator::capabilities::CAPABILITY_ALL_STANDARD};
use reifydb_core::interface::{catalog::flow::FlowNodeId, change::Change};
use reifydb_sdk::{
	error::Result as SdkResult,
	operator::{
		FFIOperator, FFIOperatorMetadata, change::BorrowedChange, column::OperatorColumn,
		context::OperatorContext,
	},
	state::cache::StateCache,
	testing::{builders::TestChangeBuilder, harness::TestHarnessBuilder},
};
use reifydb_type::value::{Value, row_number::RowNumber};
use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Serialize, Deserialize)]
struct SumState {
	sum: i64,
	count: u64,
}

struct SumAgg {
	cache: Rc<RefCell<StateCache<i64, SumState>>>,
}

impl FFIOperatorMetadata for SumAgg {
	const NAME: &'static str = "sum_agg_bench";
	const API: u32 = 1;
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "Bench fixture: per-key Int8 sum aggregate via StateCache";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;
}

impl FFIOperator for SumAgg {
	fn new(_id: FlowNodeId, _config: &HashMap<String, Value>) -> SdkResult<Self> {
		Ok(Self {
			cache: Rc::new(RefCell::new(StateCache::new(16_384))),
		})
	}

	fn apply(&mut self, ctx: &mut OperatorContext, input: BorrowedChange<'_>) -> SdkResult<()> {
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
			let mut cache = self.cache.borrow_mut();
			for &v in int_slice {
				cache.update(ctx, &v, |s| {
					s.sum += v;
					s.count += 1;
					Ok(())
				})?;
			}
		}
		Ok(())
	}

	fn pull(&mut self, _ctx: &mut OperatorContext, _row_numbers: &[RowNumber]) -> SdkResult<()> {
		Ok(())
	}

	fn flush_state(&mut self, ctx: &mut OperatorContext) -> SdkResult<()> {
		self.cache.borrow_mut().flush(ctx)
	}
}

fn bench_ffi_aggregate_flush(distinct_keys: usize, batches: usize, rows_per_batch: usize) {
	let mut harness =
		TestHarnessBuilder::<SumAgg>::new().with_node_id(FlowNodeId(1)).build().expect("build harness");

	let mut inputs: Vec<Change> = Vec::with_capacity(batches);
	for b in 0..batches {
		let mut tcb = TestChangeBuilder::new();
		for r in 0..rows_per_batch {
			let key = ((b * rows_per_batch + r) % distinct_keys) as i64;
			tcb = tcb.insert_row(((b * rows_per_batch + r) as u64) + 1, vec![Value::Int8(key)]);
		}
		inputs.push(tcb.build());
	}

	let pre_flush_state_len = harness.state().len();
	assert_eq!(pre_flush_state_len, 0, "harness state should be empty before any apply");

	let start = Instant::now();
	let (_, counts) = with_counting(|| {
		for input in inputs {
			let _ = harness.apply(input).expect("apply");
		}
	});
	let apply_elapsed = start.elapsed();

	let mid_state_len = harness.state().len();
	assert_eq!(mid_state_len, 0, "post-apply pre-flush: storage should still be empty (dirty-only)");

	let flush_start = Instant::now();
	let (_, flush_counts) = with_counting(|| {
		let mut ctx = harness.create_operator_context();
		harness.operator_mut().flush_state(&mut ctx).expect("flush_state");
	});
	let flush_elapsed = flush_start.elapsed();

	let post_flush_state_len = harness.state().len();
	assert_eq!(
		post_flush_state_len, distinct_keys,
		"post-flush: storage should hold exactly `distinct_keys` entries (one per unique key)"
	);

	let apply_allocs_per_batch = counts.allocs as f64 / batches as f64;
	let apply_bytes_per_batch = counts.bytes_allocated as f64 / batches as f64;
	println!(
		"  keys={:<5} batches={:<4} rows/batch={:<4}  apply: {:?} ({:.0}/batch allocs={:.1}, bytes={:.0})",
		distinct_keys,
		batches,
		rows_per_batch,
		apply_elapsed,
		apply_elapsed.as_nanos() as f64 / batches as f64,
		apply_allocs_per_batch,
		apply_bytes_per_batch
	);
	println!(
		"            flush: {:?} ({} allocs, {} bytes for {} keys = {:.0} bytes/key)",
		flush_elapsed,
		flush_counts.allocs,
		flush_counts.bytes_allocated,
		distinct_keys,
		flush_counts.bytes_allocated as f64 / distinct_keys as f64
	);
}

fn main() {
	println!("\n[ffi-aggregate-flush] verifies StateCache dirty-marking + flush_state");
	for &(keys, batches, rows) in &[(64usize, 100usize, 16usize), (1024, 100, 64), (10_000, 100, 100)] {
		bench_ffi_aggregate_flush(keys, batches, rows);
	}
	println!("\nDone.");
}
