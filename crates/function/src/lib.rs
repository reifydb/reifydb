// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{
	Column,
	columns::Columns,
	data::ColumnData,
	view::group_by::{GroupByView, GroupKey},
};
use reifydb_transaction::standard::StandardTransaction;
use reifydb_type::Result;

pub mod blob;
pub mod flow;
pub mod math;
pub mod registry;
pub mod series;
pub mod subscription;
pub mod text;

pub struct GeneratorContext<'a> {
	pub params: Columns,
	pub txn: &'a mut StandardTransaction<'a>,
}

pub trait GeneratorFunction: Send + Sync {
	fn generate<'a>(&self, ctx: GeneratorContext<'a>) -> Result<Columns>;
}

pub struct ScalarFunctionContext<'a> {
	pub columns: &'a Columns,
	pub row_count: usize,
}

pub trait ScalarFunction: Send + Sync {
	fn scalar<'a>(&'a self, ctx: ScalarFunctionContext<'a>) -> Result<ColumnData>;
}

pub struct AggregateFunctionContext<'a> {
	pub column: &'a Column,
	pub groups: &'a GroupByView,
}

pub trait AggregateFunction: Send + Sync {
	fn aggregate<'a>(&'a mut self, ctx: AggregateFunctionContext<'a>) -> Result<()>;
	fn finalize(&mut self) -> Result<(Vec<GroupKey>, ColumnData)>;
}
