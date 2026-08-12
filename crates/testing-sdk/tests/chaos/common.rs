// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(dead_code)]

use reifydb_codec::{
	row::shape::{RowFamily, RowShape, RowShapeField},
	tag::ValueKind,
};
use reifydb_core::interface::{catalog::flow::OperatorId, change::DiffType, flow::OperatorCapability};
use reifydb_sdk::{
	common::extern_c::binding::builder::{ColumnsBuilder, CommittedColumn},
	error::Result,
	flow::operator::{
		OperatorMetadata,
		change::{BorrowedChange, BorrowedColumns},
		column::operator::OperatorColumn,
		extern_c::binding::{context::ExternCContext, operator::ExternCOperator},
	},
};
use reifydb_testing_chaos::operator::{event::ChaosBatch, view::MaterializedView};
use reifydb_testing_sdk::chaos::{context::ChaosContext, materialize::materialize_batches};
use reifydb_value::{
	config::Config,
	value::{row_number::RowNumber, value_type::ValueType},
};

pub struct PassthroughOperator;

impl OperatorMetadata for PassthroughOperator {
	const NAME: &'static str = "chaos_passthrough";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "echoes every input diff back via ctx.builder";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl ExternCOperator for PassthroughOperator {
	fn new(_id: OperatorId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, ctx: &mut ExternCContext, input: BorrowedChange<'_>) -> Result<()> {
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

/// The point is what this does NOT break: both copies carry identical values, so the
/// materialized table still equals the identity oracle exactly. Only the fold over row
/// numbers can see the row was published twice.
pub struct DoubleInsertOperator;

impl OperatorMetadata for DoubleInsertOperator {
	const NAME: &'static str = "chaos_double_insert";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "echoes every Insert twice under the same row numbers";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl ExternCOperator for DoubleInsertOperator {
	fn new(_id: OperatorId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, ctx: &mut ExternCContext, input: BorrowedChange<'_>) -> Result<()> {
		let mut builder = ctx.builder();
		for diff in input.diffs() {
			match diff.kind() {
				DiffType::Insert => {
					emit_insert(&mut builder, &diff.post())?;
					emit_insert(&mut builder, &diff.post())?;
				}
				DiffType::Update => emit_update(&mut builder, &diff.pre(), &diff.post())?,
				DiffType::Remove => emit_remove(&mut builder, &diff.pre())?,
			}
		}
		Ok(())
	}
}

/// Known-bad operator for the divergence suite: it must diverge from the identity oracle
/// whenever the chaos sequence emits a Remove.
pub struct SwallowsRemoveOperator;

impl OperatorMetadata for SwallowsRemoveOperator {
	const NAME: &'static str = "chaos_swallows_remove";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "passthrough except Remove is silently dropped";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl ExternCOperator for SwallowsRemoveOperator {
	fn new(_id: OperatorId, _config: &Config) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, ctx: &mut ExternCContext, input: BorrowedChange<'_>) -> Result<()> {
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
			// SAFETY: dst is non-null and the preceding grow() sized it to at least
			// data_bytes.len(); source and destination are distinct allocations.
			unsafe {
				core::ptr::copy_nonoverlapping(data_bytes.as_ptr(), dst, data_bytes.len());
			}
		}
		if matches!(
			type_code,
			ValueKind::Utf8
				| ValueKind::Blob | ValueKind::Int
				| ValueKind::Uint | ValueKind::Decimal
				| ValueKind::Any | ValueKind::DictionaryId
		) {
			let off = col.offsets();
			let dst_off = active.offsets_ptr();
			if !dst_off.is_null() && !off.is_empty() {
				// SAFETY: dst_off is non-null and the builder sizes the offsets region
				// from the same row count off was read at; the buffers do not alias.
				unsafe {
					core::ptr::copy_nonoverlapping(off.as_ptr(), dst_off, off.len());
				}
			}
		}
		let bitvec = col.defined_bitvec();
		if !bitvec.is_empty() {
			let dst_bv = active.bitvec_ptr();
			if !dst_bv.is_null() {
				// SAFETY: dst_bv is non-null and the builder sizes the bitvec from the
				// same row count bitvec was read at; the buffers do not alias.
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

pub fn simple_kv_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			RowShapeField::unconstrained("k", ValueType::Uint8),
			RowShapeField::unconstrained("v", ValueType::Float8),
		],
	)
}

pub fn wide_shape() -> RowShape {
	RowShape::new(
		RowFamily::Table,
		vec![
			RowShapeField::unconstrained("base", ValueType::Utf8),
			RowShapeField::unconstrained("quote", ValueType::Utf8),
			RowShapeField::unconstrained("slot", ValueType::Uint8),
			RowShapeField::unconstrained("vol", ValueType::Float8),
			RowShapeField::unconstrained("price", ValueType::Float8),
		],
	)
}

/// Identity oracle: the materialized state is exactly the events that came in.
pub fn passthrough_oracle(
	output_key_columns: Vec<String>,
) -> impl Fn(&ChaosContext, &[ChaosBatch]) -> MaterializedView + Send + Sync + 'static {
	move |_ctx, batches| materialize_batches(batches, &output_key_columns)
}
