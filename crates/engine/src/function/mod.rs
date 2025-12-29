// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::{Column, ColumnData, Columns, GroupByView, GroupKey};

use crate::execute::ExecutionContext;

pub mod blob;
pub mod flow_node_type;
pub mod generator;
pub mod math;
mod registry;
pub mod text;

pub use registry::{Functions, FunctionsBuilder};

use crate::execute::Executor;

pub struct ScalarFunctionContext<'a> {
	pub columns: &'a Columns,
	pub row_count: usize,
}

pub trait ScalarFunction: Send + Sync {
	fn scalar<'a>(&'a self, ctx: ScalarFunctionContext<'a>) -> crate::Result<ColumnData>;
}

pub struct AggregateFunctionContext<'a> {
	pub column: &'a Column,
	pub groups: &'a GroupByView,
}

pub trait AggregateFunction: Send + Sync {
	fn aggregate<'a>(&'a mut self, ctx: AggregateFunctionContext<'a>) -> crate::Result<()>;

	fn finalize(&mut self) -> crate::Result<(Vec<GroupKey>, ColumnData)>;
}

pub struct GeneratorContext {
	pub params: Columns,
	pub execution: ExecutionContext,
	pub executor: Executor,
}

pub trait GeneratorFunction: Send + Sync {
	fn generate<'a>(&self, ctx: GeneratorContext) -> crate::Result<Columns>;
}
