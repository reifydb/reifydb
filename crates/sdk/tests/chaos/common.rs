// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#![allow(dead_code)]

use std::collections::HashMap;

use reifydb_abi::{data::column::ColumnTypeCode, flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_core::{
	encoded::shape::{RowShape, RowShapeField},
	interface::catalog::flow::FlowNodeId,
};
use reifydb_sdk::{
	config::Config,
	error::Result,
	operator::{
		FFIOperator, OperatorMetadata,
		builder::{ColumnsBuilder, CommittedColumn},
		change::{BorrowedChange, BorrowedColumns},
		column::operator::OperatorColumn,
		context::ffi::FFIOperatorContext,
	},
	testing::chaos::{
		context::ChaosContext, event::ChaosBatch, materialize::materialize_batches, oracle::MaterializedTable,
	},
};
use reifydb_type::value::{Value, row_number::RowNumber, r#type::Type};

/// Operator that echoes every input diff back unchanged through
/// `ctx.builder()`. Modeled on `tests/ffi/common.rs:39-106`. Used by every
/// must-match scenario in the chaos integration suite.
pub struct PassthroughOperator;

impl OperatorMetadata for PassthroughOperator {
	const NAME: &'static str = "chaos_passthrough";
	const API: u32 = 1;
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "echoes every input diff back via ctx.builder";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl FFIOperator for PassthroughOperator {
	fn new(_id: FlowNodeId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, ctx: &mut FFIOperatorContext, input: BorrowedChange<'_>) -> Result<()> {
		let mut builder = ctx.builder();
		for diff in input.diffs() {
			match diff.kind() {
				DiffType::Insert => emit_insert(&mut builder, &diff.post())?,
				DiffType::Update => emit_update(&mut builder, &diff.pre(), &diff.post())?,
				DiffType::Remove => emit_remove(&mut builder, &diff.pre())?,
			}
		}
		Ok(())
	}
}

/// Operator that echoes Insert and Update but silently drops Remove. Used by
/// the divergence suite to demonstrate the harness catching a real-bug-class
/// in the operator (forgetting to handle a diff kind). Identity oracle vs
/// this operator must diverge whenever the chaos sequence emits a Remove.
pub struct SwallowsRemoveOperator;

impl OperatorMetadata for SwallowsRemoveOperator {
	const NAME: &'static str = "chaos_swallows_remove";
	const API: u32 = 1;
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "passthrough except Remove is silently dropped";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl FFIOperator for SwallowsRemoveOperator {
	fn new(_id: FlowNodeId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, ctx: &mut FFIOperatorContext, input: BorrowedChange<'_>) -> Result<()> {
		let mut builder = ctx.builder();
		for diff in input.diffs() {
			match diff.kind() {
				DiffType::Insert => emit_insert(&mut builder, &diff.post())?,
				DiffType::Update => emit_update(&mut builder, &diff.pre(), &diff.post())?,
				DiffType::Remove => {} // intentional bug: drop Removes
			}
		}
		Ok(())
	}
}

fn emit_insert(builder: &mut ColumnsBuilder<'_>, post: &BorrowedColumns<'_>) -> Result<()> {
	let (cols, names) = byte_clone_columns(builder, post)?;
	let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
	let row_numbers: Vec<RowNumber> = post.row_numbers().iter().copied().map(RowNumber).collect();
	builder.emit_insert(&cols, &names_ref, &row_numbers)?;
	Ok(())
}

fn emit_update(builder: &mut ColumnsBuilder<'_>, pre: &BorrowedColumns<'_>, post: &BorrowedColumns<'_>) -> Result<()> {
	let (pre_cols, pre_names) = byte_clone_columns(builder, pre)?;
	let (post_cols, post_names) = byte_clone_columns(builder, post)?;
	let pre_names_ref: Vec<&str> = pre_names.iter().map(|s| s.as_str()).collect();
	let post_names_ref: Vec<&str> = post_names.iter().map(|s| s.as_str()).collect();
	let pre_row_numbers: Vec<RowNumber> = pre.row_numbers().iter().copied().map(RowNumber).collect();
	let post_row_numbers: Vec<RowNumber> = post.row_numbers().iter().copied().map(RowNumber).collect();
	builder.emit_update(
		&pre_cols,
		&pre_names_ref,
		pre.row_count(),
		&pre_row_numbers,
		&post_cols,
		&post_names_ref,
		post.row_count(),
		&post_row_numbers,
	)?;
	Ok(())
}

fn emit_remove(builder: &mut ColumnsBuilder<'_>, pre: &BorrowedColumns<'_>) -> Result<()> {
	let (cols, names) = byte_clone_columns(builder, pre)?;
	let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
	let row_numbers: Vec<RowNumber> = pre.row_numbers().iter().copied().map(RowNumber).collect();
	builder.emit_remove(&cols, &names_ref, &row_numbers)?;
	Ok(())
}

/// Acquire matching builders for each input column, byte-copy the data,
/// offsets, and defined-bitvec across, commit, and return the committed
/// handles + column names. Verbatim from `tests/ffi/common.rs:108-159`.
fn byte_clone_columns(
	builder: &mut ColumnsBuilder<'_>,
	cols: &BorrowedColumns<'_>,
) -> Result<(Vec<CommittedColumn>, Vec<String>)> {
	let row_count = cols.row_count();
	let mut committed: Vec<CommittedColumn> = Vec::new();
	let mut names: Vec<String> = Vec::new();
	for col in cols.columns() {
		let type_code = col.type_code();
		let data_bytes = col.data_bytes();
		let active = builder.acquire(type_code, row_count.max(1))?;
		active.grow(data_bytes.len().max(row_count))?;
		let dst = active.data_ptr();
		if !dst.is_null() && !data_bytes.is_empty() {
			unsafe {
				core::ptr::copy_nonoverlapping(data_bytes.as_ptr(), dst, data_bytes.len());
			}
		}
		if matches!(
			type_code,
			ColumnTypeCode::Utf8
				| ColumnTypeCode::Blob | ColumnTypeCode::Int
				| ColumnTypeCode::Uint | ColumnTypeCode::Decimal
				| ColumnTypeCode::Any | ColumnTypeCode::DictionaryId
		) {
			let off = col.offsets();
			let dst_off = active.offsets_ptr();
			if !dst_off.is_null() && !off.is_empty() {
				unsafe {
					core::ptr::copy_nonoverlapping(off.as_ptr(), dst_off, off.len());
				}
			}
		}
		let bitvec = col.defined_bitvec();
		if !bitvec.is_empty() {
			let dst_bv = active.bitvec_ptr();
			if !dst_bv.is_null() {
				unsafe {
					core::ptr::copy_nonoverlapping(bitvec.as_ptr(), dst_bv, bitvec.len());
				}
			}
		}
		let c = active.commit(row_count)?;
		committed.push(c);
		names.push(col.name().to_string());
	}
	Ok((committed, names))
}

/// `(k: Uint8, v: Float8)` shape used as input + output for most scenarios.
/// Output key projects on `k`.
pub fn simple_kv_shape() -> RowShape {
	RowShape::new(vec![
		RowShapeField::unconstrained("k", Type::Uint8),
		RowShapeField::unconstrained("v", Type::Float8),
	])
}

/// `(base, quote, slot, vol, price)` shape for multi-column key strategies
/// and tolerance scenarios. Output key projects on `(base, quote, slot)`.
pub fn wide_shape() -> RowShape {
	RowShape::new(vec![
		RowShapeField::unconstrained("base", Type::Utf8),
		RowShapeField::unconstrained("quote", Type::Utf8),
		RowShapeField::unconstrained("slot", Type::Uint8),
		RowShapeField::unconstrained("vol", Type::Float8),
		RowShapeField::unconstrained("price", Type::Float8),
	])
}

/// Identity oracle: whatever events came in, that is the materialized state.
/// Used by every passthrough scenario.
///
/// Returns a `Fn` closure rather than capturing by reference so the result
/// satisfies `Send + Sync + 'static` (the bound on `ChaosHarnessBuilder::with_oracle`).
pub fn passthrough_oracle(
	output_key_columns: Vec<String>,
) -> impl Fn(&ChaosContext, &[ChaosBatch]) -> MaterializedTable + Send + Sync + 'static {
	move |_ctx, batches| materialize_batches(batches, &output_key_columns)
}
