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
use reifydb_type::fragment::Fragment;

pub mod blob;
pub mod clock;
pub mod date;
pub mod error;
pub mod flow;
pub mod math;
pub mod meta;
pub mod registry;
pub mod series;
pub mod subscription;
pub mod text;

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
