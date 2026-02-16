// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{
	Column,
	columns::Columns,
	data::ColumnData,
	view::group_by::{GroupByView, GroupKey},
};
use reifydb_runtime::clock::Clock;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, util::bitvec::BitVec};

pub mod blob;
pub mod clock;
pub mod date;
pub mod datetime;
pub mod duration;
pub mod error;
pub mod flow;
pub mod is;
pub mod math;
pub mod meta;
pub mod registry;
pub mod series;
pub mod subscription;
pub mod text;
pub mod time;

use error::{AggregateFunctionResult, GeneratorFunctionResult, ScalarFunctionResult};
use reifydb_catalog::catalog::Catalog;

pub struct GeneratorContext<'a> {
	pub fragment: Fragment,
	pub params: Columns,
	pub txn: &'a mut Transaction<'a>,
	pub catalog: &'a Catalog,
}

pub trait GeneratorFunction: Send + Sync {
	fn generate<'a>(&self, ctx: GeneratorContext<'a>) -> GeneratorFunctionResult<Columns>;
}

pub struct ScalarFunctionContext<'a> {
	pub fragment: Fragment,
	pub columns: &'a Columns,
	pub row_count: usize,
	pub clock: &'a Clock,
}
pub trait ScalarFunction: Send + Sync {
	fn scalar<'a>(&'a self, ctx: ScalarFunctionContext<'a>) -> ScalarFunctionResult<ColumnData>;
}

pub struct AggregateFunctionContext<'a> {
	pub fragment: Fragment,
	pub column: &'a Column,
	pub groups: &'a GroupByView,
}

pub trait AggregateFunction: Send + Sync {
	fn aggregate<'a>(&'a mut self, ctx: AggregateFunctionContext<'a>) -> AggregateFunctionResult<()>;
	fn finalize(&mut self) -> AggregateFunctionResult<(Vec<GroupKey>, ColumnData)>;
}

/// Helper for scalar functions to opt into Option propagation.
///
/// If any argument column is `ColumnData::Option` or `ColumnData::Undefined`,
/// this unwraps the Option wrappers, calls `func.scalar()` recursively on the
/// inner data, and re-wraps the result with the combined bitvec.
///
/// Returns `None` when no Option columns are present (the caller should
/// proceed with its normal typed logic).
///
/// Functions that need raw access to Options (e.g. `is::some`, `is::none`)
/// simply don't call this helper.
pub fn propagate_options(
	func: &dyn ScalarFunction,
	ctx: &ScalarFunctionContext,
) -> Option<ScalarFunctionResult<ColumnData>> {
	let has_option =
		ctx.columns.iter().any(|c| matches!(c.data(), ColumnData::Option { .. } | ColumnData::Undefined(_)));
	if !has_option {
		return None;
	}

	// Short-circuit: if any column is entirely Undefined, the result is entirely undefined.
	// This avoids infinite recursion since unwrap_option() does not unwrap Undefined.
	if ctx.columns.iter().any(|c| matches!(c.data(), ColumnData::Undefined(_))) {
		return Some(Ok(ColumnData::undefined(ctx.row_count)));
	}

	let mut combined_bv: Option<BitVec> = None;
	let mut unwrapped = Vec::with_capacity(ctx.columns.len());
	for col in ctx.columns.iter() {
		let (inner, bv) = col.data().unwrap_option();
		if let Some(bv) = bv {
			combined_bv = Some(match combined_bv {
				Some(existing) => existing.and(bv),
				None => bv.clone(),
			});
		}
		unwrapped.push(Column::new(col.name().clone(), inner.clone()));
	}

	let unwrapped_columns = Columns::new(unwrapped);
	let result = func.scalar(ScalarFunctionContext {
		fragment: ctx.fragment.clone(),
		columns: &unwrapped_columns,
		row_count: ctx.row_count,
		clock: ctx.clock,
	});

	Some(result.map(|data| match combined_bv {
		Some(bv) => ColumnData::Option {
			inner: Box::new(data),
			bitvec: bv,
		},
		None => data,
	}))
}
