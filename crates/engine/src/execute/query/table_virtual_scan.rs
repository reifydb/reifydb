// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::value::column::headers::ColumnHeaders;
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::{Batch, ExecutionContext, QueryNode},
	table_virtual::{TableVirtual, TableVirtualContext},
};

pub(crate) struct VirtualScanNode<'a> {
	virtual_table: Box<dyn TableVirtual<'a>>,
	context: Option<Arc<ExecutionContext<'a>>>,
	headers: ColumnHeaders<'a>,
	table_context: Option<TableVirtualContext<'a>>,
}

impl<'a> VirtualScanNode<'a> {
	pub fn new(
		virtual_table: Box<dyn TableVirtual<'a>>,
		context: Arc<ExecutionContext<'a>>,
		table_context: TableVirtualContext<'a>,
	) -> crate::Result<Self> {
		let def = virtual_table.definition();

		let headers = ColumnHeaders {
			columns: def.columns.iter().map(|col| Fragment::owned_internal(&col.name)).collect(),
		};

		Ok(Self {
			virtual_table,
			context: Some(context),
			headers,
			table_context: Some(table_context),
		})
	}
}

impl<'a> QueryNode<'a> for VirtualScanNode<'a> {
	fn initialize(&mut self, rx: &mut StandardTransaction<'a>, _ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		let ctx = self.table_context.take().unwrap_or_else(|| TableVirtualContext::Basic {
			params: self.context.as_ref().unwrap().params.clone(),
		});
		self.virtual_table.initialize(rx, ctx)?;
		Ok(())
	}

	fn next(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		debug_assert!(self.context.is_some(), "VirtualScanNode::next() called before initialize()");
		self.virtual_table.next(rx)
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		Some(self.headers.clone())
	}
}
