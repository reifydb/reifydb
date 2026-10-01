// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![allow(dead_code)]

use reifydb_codec::row::shape::{RowFamily, RowShape, RowShapeField};
use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	operator_with::ApplyWith,
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		NostateOperator, OperatorMetadata,
		column::{operator::OperatorColumn, row::Row},
		context::{GuestContext, GuestEmitContext, Nostate},
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
	row,
};
use reifydb_testing_chaos::operator::{event::ChaosBatch, view::MaterializedView};
use reifydb_testing_sdk::chaos::{context::ChaosContext, materialize::materialize_batches};
use reifydb_value::{
	config::ExtensionParams,
	value::{diff_type::DiffType, row_number::RowNumber, value_type::ValueType},
};

pub struct KvRow {
	k: Option<u64>,
	v: Option<f64>,
}

row!(KvRow {
	k: Option<u64>,
	v: Option<f64>
});

pub struct PassthroughOperator;

impl OperatorMetadata for PassthroughOperator {
	const NAME: &'static str = "chaos_passthrough";
	const VERSION: &'static str = "1.0.0";
	const DESCRIPTION: &'static str = "echoes every row of every input diff back";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl NostateOperator for PassthroughOperator {
	fn create(_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Nostate>, change: impl ChangeView) -> Result<()> {
		for index in 0..change.diff_count() {
			let diff = change.diff(index).expect("every counted diff resolves");
			match diff.kind() {
				DiffType::Insert => emit_insert(ctx, &diff)?,
				DiffType::Update => emit_update(ctx, &diff)?,
				DiffType::Remove => emit_remove(ctx, &diff)?,
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

impl NostateOperator for DoubleInsertOperator {
	fn create(_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Nostate>, change: impl ChangeView) -> Result<()> {
		for index in 0..change.diff_count() {
			let diff = change.diff(index).expect("every counted diff resolves");
			match diff.kind() {
				DiffType::Insert => {
					emit_insert(ctx, &diff)?;
					emit_insert(ctx, &diff)?;
				}
				DiffType::Update => emit_update(ctx, &diff)?,
				DiffType::Remove => emit_remove(ctx, &diff)?,
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

impl NostateOperator for SwallowsRemoveOperator {
	fn create(_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(Self)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Nostate>, change: impl ChangeView) -> Result<()> {
		for index in 0..change.diff_count() {
			let diff = change.diff(index).expect("every counted diff resolves");
			match diff.kind() {
				DiffType::Insert => emit_insert(ctx, &diff)?,
				DiffType::Update => emit_update(ctx, &diff)?,
				DiffType::Remove => {} // intentional bug: drop Removes
			}
		}
		Ok(())
	}
}

fn rows_of(columns: &impl ColumnsView) -> Result<(Vec<KvRow>, Vec<RowNumber>)> {
	let mut rows = Vec::with_capacity(columns.row_count());
	let mut numbers = Vec::with_capacity(columns.row_count());
	for position in 0..columns.row_count() {
		let row = columns.row(position).expect("every counted row resolves");
		rows.push(KvRow::decode_from(&row)?.expect("a row of optional cells always decodes"));
		numbers.push(row.row_number().expect("every chaos row is numbered"));
	}
	Ok((rows, numbers))
}

fn emit_insert(ctx: &mut impl GuestEmitContext, diff: &impl DiffView) -> Result<()> {
	let (rows, numbers) = rows_of(&diff.post().expect("an insert carries a post batch"))?;
	ctx.emit_insert(&rows, &numbers)
}

fn emit_update(ctx: &mut impl GuestEmitContext, diff: &impl DiffView) -> Result<()> {
	let (pre, _) = rows_of(&diff.pre().expect("an update carries a pre batch"))?;
	let (post, numbers) = rows_of(&diff.post().expect("an update carries a post batch"))?;
	ctx.emit_update(&pre, &post, &numbers)
}

fn emit_remove(ctx: &mut impl GuestEmitContext, diff: &impl DiffView) -> Result<()> {
	let (rows, numbers) = rows_of(&diff.pre().expect("a remove carries a pre batch"))?;
	ctx.emit_remove(&rows, &numbers)
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
