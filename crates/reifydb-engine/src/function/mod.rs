// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use registry::Functions;

use crate::{
	GroupByView, GroupKey,
	columnar::{Column, ColumnData, Columns},
};

pub mod blob;
pub mod math;
mod registry;

pub struct ScalarFunctionContext<'a> {
	pub columns: &'a Columns,
	pub row_count: usize,
}

pub trait ScalarFunction: Send + Sync {
	fn scalar<'a>(
		&'a self,
		ctx: ScalarFunctionContext<'a>,
	) -> crate::Result<ColumnData>;
}

pub struct AggregateFunctionContext<'a> {
	pub column: &'a Column,
	pub groups: &'a GroupByView,
}

pub trait AggregateFunction: Send + Sync {
	fn aggregate<'a>(
		&'a mut self,
		ctx: AggregateFunctionContext<'a>,
	) -> crate::Result<()>;

	fn finalize(&mut self) -> crate::Result<(Vec<GroupKey>, ColumnData)>;
}
