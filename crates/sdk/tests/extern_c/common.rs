// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(dead_code)]

use reifydb_codec::tag::ValueKind;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff, Diffs},
		flow::OperatorCapability,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
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
use reifydb_testing_sdk::harness::ExternCOperatorHarnessBuilder;
use reifydb_value::{
	config::Config,
	fragment::Fragment,
	value::{Value, datetime::DateTime, diff_type::DiffType, row_number::RowNumber, system_columns::SystemColumns},
};

/// Echoing every diff back drives both the input borrow path and the output builder path in a single apply, which
/// is what makes a round trip through this operator a test of the whole extern-C column ABI.
pub struct PassthroughOperator;

impl OperatorMetadata for PassthroughOperator {
	const NAME: &'static str = "extern_c_round_trip_passthrough";
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
				DiffType::Insert => {
					let post = diff.post();
					let (cols, names) = byte_clone_columns(&mut builder, &post)?;
					let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
					let row_numbers: Vec<RowNumber> =
						post.row_numbers().iter().copied().map(RowNumber).collect();
					builder.emit_insert(&cols, &names_ref, &row_numbers)?;
				}
				DiffType::Update => {
					let pre = diff.pre();
					let post = diff.post();
					let (pre_cols, pre_names) = byte_clone_columns(&mut builder, &pre)?;
					let (post_cols, post_names) = byte_clone_columns(&mut builder, &post)?;
					let pre_names_ref: Vec<&str> = pre_names.iter().map(|s| s.as_str()).collect();
					let post_names_ref: Vec<&str> = post_names.iter().map(|s| s.as_str()).collect();
					let pre_row_numbers: Vec<RowNumber> =
						pre.row_numbers().iter().copied().map(RowNumber).collect();
					let post_row_numbers: Vec<RowNumber> =
						post.row_numbers().iter().copied().map(RowNumber).collect();
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
				}
				DiffType::Remove => {
					let pre = diff.pre();
					let (cols, names) = byte_clone_columns(&mut builder, &pre)?;
					let names_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
					let row_numbers: Vec<RowNumber> =
						pre.row_numbers().iter().copied().map(RowNumber).collect();
					builder.emit_remove(&cols, &names_ref, &row_numbers)?;
				}
			}
		}
		Ok(())
	}
}

/// Copies at the byte level rather than value level, so a defect in the marshal encoding survives the copy and is
/// visible in the round-tripped output instead of being normalised away.
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
			// SAFETY: dst is non-null and the preceding grow() sized the data region to at least
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
				// SAFETY: dst_off is non-null and the builder sized the offsets region from the
				// same row count off was read at; the buffers do not alias.
				unsafe {
					core::ptr::copy_nonoverlapping(off.as_ptr(), dst_off, off.len());
				}
			}
		}
		let bitvec = col.defined_bitvec();
		if !bitvec.is_empty() {
			let dst_bv = active.bitvec_ptr();
			if !dst_bv.is_null() {
				// SAFETY: dst_bv is non-null and the builder allocates the bitvec at
				// row_count.div_ceil(8) bytes, which is bitvec.len(); the buffers do not alias.
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

pub fn round_trip_column(name: &str, input: ColumnBuffer) -> ColumnBuffer {
	let n = input.len();
	let row_numbers: Vec<RowNumber> = (1..=(n as u64).max(1)).map(RowNumber).take(n).collect();
	let now = DateTime::default();
	let timestamps: Vec<DateTime> = vec![now; n];
	let cols = vec![ColumnWithName::new(Fragment::internal(name), input)];
	let columns = Columns::with_system(
		cols,
		SystemColumns::new(row_numbers, Vec::new(), timestamps.clone(), timestamps.clone(), timestamps),
	);

	let mut diffs: Diffs = Diffs::new();
	diffs.push(Diff::insert(columns));
	let change = Change::from_flow(OperatorId(1), CommitVersion(1), diffs, now);

	let mut harness = ExternCOperatorHarnessBuilder::<PassthroughOperator>::new()
		.with_node_id(OperatorId(1))
		.build()
		.expect("build harness");
	let output = harness.apply(change).expect("apply");

	assert_eq!(output.diffs.len(), 1, "expected exactly one output diff");
	let out_columns = match &output.diffs[0] {
		Diff::Insert {
			post,
			..
		} => post,
		Diff::Update {
			post,
			..
		} => post,
		Diff::Remove {
			pre,
			..
		} => pre,
	};
	assert_eq!(out_columns.columns.len(), 1, "expected exactly one output column");
	out_columns.columns[0].clone()
}

pub fn assert_column_eq(label: &str, expected: &ColumnBuffer, actual: &ColumnBuffer) {
	assert_eq!(
		expected.get_type(),
		actual.get_type(),
		"{}: type mismatch: expected {:?}, got {:?}",
		label,
		expected.get_type(),
		actual.get_type()
	);
	assert_eq!(
		expected.len(),
		actual.len(),
		"{}: row count mismatch: expected {}, got {}",
		label,
		expected.len(),
		actual.len()
	);
	let exp: Vec<Value> = expected.iter().collect();
	let act: Vec<Value> = actual.iter().collect();
	for (i, (e, a)) in exp.iter().zip(act.iter()).enumerate() {
		let matches = values_match(e, a);
		if !matches {
			panic!("{}: row {}: expected {:?}, got {:?}", label, i, e, a);
		}
	}
}

/// Bit equality rather than `==` for floats, so a round trip that turns -0.0 into +0.0 or flattens a sub-normal
/// is caught; NaN is the one case compared by classification instead.
fn values_match(a: &Value, b: &Value) -> bool {
	use Value::*;
	match (a, b) {
		(Float4(av), Float4(bv)) => {
			let af: f32 = (*av).into();
			let bf: f32 = (*bv).into();
			(af.is_nan() && bf.is_nan()) || af.to_bits() == bf.to_bits()
		}
		(Float8(av), Float8(bv)) => {
			let af: f64 = (*av).into();
			let bf: f64 = (*bv).into();
			(af.is_nan() && bf.is_nan()) || af.to_bits() == bf.to_bits()
		}
		_ => a == b,
	}
}
