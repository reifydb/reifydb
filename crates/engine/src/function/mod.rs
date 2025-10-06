// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::value::column::{Column, ColumnData, Columns, GroupByView, GroupKey};

use crate::{StandardTransaction, execute::ExecutionContext};

pub mod blob;
pub mod generator;
pub mod math;
mod registry;

pub use registry::{Functions, FunctionsBuilder};

pub struct ScalarFunctionContext<'a> {
	pub columns: &'a Columns<'a>,
	pub row_count: usize,
}

pub trait ScalarFunction: Send + Sync {
	fn scalar<'a>(&'a self, ctx: ScalarFunctionContext<'a>) -> crate::Result<ColumnData>;
}

pub struct AggregateFunctionContext<'a> {
	pub column: &'a Column<'a>,
	pub groups: &'a GroupByView,
}

pub trait AggregateFunction: Send + Sync {
	fn aggregate<'a>(&'a mut self, ctx: AggregateFunctionContext<'a>) -> crate::Result<()>;

	fn finalize(&mut self) -> crate::Result<(Vec<GroupKey>, ColumnData)>;
}

pub struct GeneratorContext<'a> {
	pub params: Columns<'a>,
	pub execution: ExecutionContext<'a>,
	pub executor: crate::execute::Executor,
}

pub trait GeneratorFunction: Send + Sync {
	fn generate<'a>(
		&self,
		txn: &mut StandardTransaction<'a>,
		ctx: GeneratorContext<'a>,
	) -> crate::Result<Columns<'a>>;
}
