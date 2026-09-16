// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	error::diagnostic::query::append_requires_deferred_view,
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::Error, fragment::Fragment};

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct UnsupportedAppendNode {
	fragment: Fragment,
}

impl UnsupportedAppendNode {
	pub fn new(fragment: Fragment) -> Self {
		Self {
			fragment,
		}
	}
}

impl QueryNode for UnsupportedAppendNode {
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Err(Error(Box::new(append_requires_deferred_view(self.fragment.clone()))))
	}

	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		Err(Error(Box::new(append_requires_deferred_view(self.fragment.clone()))))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
