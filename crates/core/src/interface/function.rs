// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::Result;

use crate::value::column::{Column, ColumnData, Columns, GroupByView, GroupKey};

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

pub struct GeneratorContext {
	pub params: Columns,
}

pub trait GeneratorFunction: Send + Sync {
	fn generate<'a>(&self, ctx: GeneratorContext) -> Result<Columns>;
}
