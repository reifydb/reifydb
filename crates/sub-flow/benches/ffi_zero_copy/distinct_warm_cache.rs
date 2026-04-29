// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Native stateful op driven through the per-txn state cache: load once on
//! first apply, hit cache on subsequent batches (Change 1 native flavor).
//! Compared against a flush-every-batch baseline to confirm the cache
//! pays off.

#![allow(clippy::needless_range_loop)]

#[path = "common.rs"]
mod common;

use std::{sync::Arc as StdArc, time::Instant};

use common::with_counting;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff, Diffs},
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_runtime::context::RuntimeContext;
use reifydb_sub_flow::{
	operator::{Operator, Operators, distinct::DistinctOperator},
	transaction::FlowTransaction,
};
use reifydb_transaction::{interceptor::interceptors::Interceptors, transaction::admin::AdminTransaction};
use reifydb_type::{
	Result as TypeResult,
	fragment::Fragment,
	util::cowvec::CowVec,
	value::{container::number::NumberContainer, datetime::DateTime, identity::IdentityId, row_number::RowNumber},
};

/// No-op stand-in for the parent operator slot of `DistinctOperator`. The
/// bench only drives Insert paths, which never read from the parent;
/// `pull` is never invoked.
struct NoOpParent;

impl Operator for NoOpParent {
	fn id(&self) -> FlowNodeId {
		FlowNodeId(0)
	}
	fn apply(&self, _txn: &mut FlowTransaction, change: Change) -> TypeResult<Change> {
		Ok(change)
	}
	fn pull(&self, _txn: &mut FlowTransaction, _rows: &[RowNumber]) -> TypeResult<Columns> {
		Ok(Columns::empty())
	}
}

fn make_distinct_op(node_id: u64) -> (DistinctOperator, TestEngine) {
	let engine = TestEngine::new();
	let routines = engine.executor().routines.clone();
	let rc = RuntimeContext::with_clock(engine.clock().clone());
	let parent: StdArc<Operators> = StdArc::new(Operators::Custom(Box::new(NoOpParent)));
	let op = DistinctOperator::new(parent, FlowNodeId(node_id), Vec::new(), routines, rc);
	(op, engine)
}

fn make_flow_txn(engine: &TestEngine) -> (FlowTransaction, AdminTransaction) {
	let admin = engine.begin_admin(IdentityId::system()).expect("begin_admin");
	let txn = FlowTransaction::deferred(
		&admin,
		CommitVersion(1),
		engine.catalog(),
		Interceptors::new(),
		engine.clock().clone(),
	);
	(txn, admin)
}

/// Build a Change with `n_rows` Int8 values cycling through `distinct_keys`.
fn build_distinct_input(distinct_keys: usize, n_rows: usize, batch_idx: usize) -> Change {
	let int_data: Vec<i64> = (0..n_rows).map(|r| ((batch_idx * n_rows + r) % distinct_keys) as i64).collect();
	let int_col = ColumnBuffer::Int8(NumberContainer::from_parts(CowVec::new(int_data)));
	let cols = vec![ColumnWithName::new(Fragment::internal("k"), int_col)];
	let row_numbers: Vec<RowNumber> =
		(1..=n_rows as u64).map(|i| RowNumber(i + (batch_idx as u64 * 1_000_000))).collect();
	let now = DateTime::default();
	let timestamps: Vec<DateTime> = vec![now; n_rows];
	let columns = Columns::with_system_columns(cols, row_numbers, timestamps.clone(), timestamps);
	let mut diffs: Diffs = Diffs::new();
	diffs.push(Diff::insert(columns));
	Change::from_flow(FlowNodeId(99), CommitVersion(1), diffs, now)
}

fn bench_distinct_warm_cache(distinct_keys: usize, batches: usize, rows_per_batch: usize) {
	// Cached run: load state once at the start of the txn, hit the cache
	// on every subsequent apply, flush once at the end (production pattern).
	let cached_bytes_per_batch;
	let cached_allocs_per_batch;
	let cached_elapsed;
	{
		let (op, engine) = make_distinct_op(1);
		let (mut txn, _admin) = make_flow_txn(&engine);
		let inputs: Vec<Change> =
			(0..batches).map(|b| build_distinct_input(distinct_keys, rows_per_batch, b)).collect();

		let warmup = build_distinct_input(distinct_keys, rows_per_batch, 999);
		let _ = op.apply(&mut txn, warmup).expect("warmup apply");

		let start = Instant::now();
		let (_, counts) = with_counting(|| {
			for input in inputs {
				let _ = op.apply(&mut txn, input).expect("apply");
			}
		});
		cached_elapsed = start.elapsed();
		cached_allocs_per_batch = counts.allocs as f64 / batches as f64;
		cached_bytes_per_batch = counts.bytes_allocated as f64 / batches as f64;
	}

	// Anti-cache run: flush after every apply. Forces the state to be
	// re-encoded and re-decoded on every batch (the pre-Change-1 baseline).
	let nocache_bytes_per_batch;
	let nocache_allocs_per_batch;
	let nocache_elapsed;
	{
		let (op, engine) = make_distinct_op(2);
		let (mut txn, _admin) = make_flow_txn(&engine);
		let inputs: Vec<Change> =
			(0..batches).map(|b| build_distinct_input(distinct_keys, rows_per_batch, b)).collect();
		let warmup = build_distinct_input(distinct_keys, rows_per_batch, 999);
		let _ = op.apply(&mut txn, warmup).expect("warmup apply");
		txn.flush_operator_states().expect("flush warmup");

		let start = Instant::now();
		let (_, counts) = with_counting(|| {
			for input in inputs {
				let _ = op.apply(&mut txn, input).expect("apply");
				txn.flush_operator_states().expect("flush per-batch");
			}
		});
		nocache_elapsed = start.elapsed();
		nocache_allocs_per_batch = counts.allocs as f64 / batches as f64;
		nocache_bytes_per_batch = counts.bytes_allocated as f64 / batches as f64;
	}

	println!("  keys={:<5} batches={:<4} rows/batch={:<4}", distinct_keys, batches, rows_per_batch,);
	println!(
		"    cached  : {:>12?} ({:>7.0} ns/batch, {:>7.1} allocs/batch, {:>9.0} bytes/batch)",
		cached_elapsed,
		cached_elapsed.as_nanos() as f64 / batches as f64,
		cached_allocs_per_batch,
		cached_bytes_per_batch,
	);
	println!(
		"    nocache : {:>12?} ({:>7.0} ns/batch, {:>7.1} allocs/batch, {:>9.0} bytes/batch)",
		nocache_elapsed,
		nocache_elapsed.as_nanos() as f64 / batches as f64,
		nocache_allocs_per_batch,
		nocache_bytes_per_batch,
	);

	let speedup_bytes = nocache_bytes_per_batch / cached_bytes_per_batch.max(1.0);
	println!("    speedup : {:.2}× bytes saved by the cache", speedup_bytes);
	assert!(
		nocache_bytes_per_batch >= cached_bytes_per_batch,
		"state cache regression: nocache run allocated less ({:.0}) than cached run ({:.0})",
		nocache_bytes_per_batch,
		cached_bytes_per_batch,
	);
}

fn main() {
	println!("\n[distinct-warm-cache] verifies per-txn state cache: load once, hit cache on subsequent batches");
	for &(keys, batches, rows) in &[(64usize, 100usize, 16usize), (1024, 100, 64), (10_000, 100, 100)] {
		bench_distinct_warm_cache(keys, batches, rows);
	}
	println!("\nDone.");
}
