// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::{error::diagnostic::operation::lookup_outside_deferred_view, value::column::headers::ColumnHeaders};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::Error, fragment::Fragment};

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct UnsupportedLookupNode {
	fragment: Fragment,
}

impl UnsupportedLookupNode {
	pub fn new(fragment: Fragment) -> Self {
		Self {
			fragment,
		}
	}
}

impl QueryNode for UnsupportedLookupNode {
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Err(Error(Box::new(lookup_outside_deferred_view(self.fragment.clone()))))
	}

	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		Err(Error(Box::new(lookup_outside_deferred_view(self.fragment.clone()))))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
