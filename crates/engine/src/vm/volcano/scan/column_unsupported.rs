// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	error::diagnostic::query::unsupported_in_column_layout,
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct UnsupportedColumnScanNode {
	what: String,
}

impl UnsupportedColumnScanNode {
	pub fn new(what: String) -> Self {
		Self {
			what,
		}
	}
}

impl QueryNode for UnsupportedColumnScanNode {
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Err(Error(Box::new(unsupported_in_column_layout(&self.what))))
	}

	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		Err(Error(Box::new(unsupported_in_column_layout(&self.what))))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
