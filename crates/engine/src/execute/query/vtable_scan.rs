// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_catalog::vtable::{VTableContext, tables::VTables};
use reifydb_core::value::column::headers::ColumnHeaders;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use crate::execute::{Batch, ExecutionContext, QueryNode};

pub(crate) struct VirtualScanNode {
	virtual_table: VTables,
	context: Option<Arc<ExecutionContext>>,
	headers: ColumnHeaders,
	table_context: Option<VTableContext>,
}

impl VirtualScanNode {
	pub fn new(
		virtual_table: VTables,
		context: Arc<ExecutionContext>,
		table_context: VTableContext,
	) -> crate::Result<Self> {
		let def = virtual_table.definition();

		let headers = ColumnHeaders {
			columns: def.columns.iter().map(|col| Fragment::internal(&col.name)).collect(),
		};

		Ok(Self {
			virtual_table,
			context: Some(context),
			headers,
			table_context: Some(table_context),
		})
	}
}

impl QueryNode for VirtualScanNode {
	#[instrument(name = "query::scan::virtual::initialize", level = "trace", skip_all)]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &ExecutionContext) -> crate::Result<()> {
		let ctx = self.table_context.take().unwrap_or_else(|| VTableContext::Basic {
			params: self.context.as_ref().unwrap().params.clone(),
		});
		self.virtual_table.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(name = "query::scan::virtual::next", level = "trace", skip_all)]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut ExecutionContext) -> crate::Result<Option<Batch>> {
		debug_assert!(self.context.is_some(), "VirtualScanNode::next() called before initialize()");
		match self.virtual_table.next(rx)? {
			Some(vtable_batch) => Ok(Some(Batch {
				columns: vtable_batch.columns,
			})),
			None => Ok(None),
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
