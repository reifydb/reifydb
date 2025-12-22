// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use std::sync::Arc;

use reifydb_core::value::column::headers::ColumnHeaders;
use reifydb_type::Fragment;
use tracing::instrument;

use crate::{
	StandardTransaction,
	execute::{Batch, ExecutionContext, QueryNode},
	table_virtual::{TableVirtual, TableVirtualContext},
};

pub(crate) struct VirtualScanNode {
	virtual_table: Box<dyn TableVirtual>,
	context: Option<Arc<ExecutionContext>>,
	headers: ColumnHeaders,
	table_context: Option<TableVirtualContext>,
}

impl VirtualScanNode {
	pub fn new(
		virtual_table: Box<dyn TableVirtual>,
		context: Arc<ExecutionContext>,
		table_context: TableVirtualContext,
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

#[async_trait]
impl QueryNode for VirtualScanNode {
	#[instrument(name = "query::scan::virtual::initialize", level = "trace", skip_all)]
	async fn initialize<'a>(&mut self, rx: &mut StandardTransaction<'a>, _ctx: &ExecutionContext) -> crate::Result<()> {
		let ctx = self.table_context.take().unwrap_or_else(|| TableVirtualContext::Basic {
			params: self.context.as_ref().unwrap().params.clone(),
		});
		self.virtual_table.initialize(rx, ctx).await?;
		Ok(())
	}

	#[instrument(name = "query::scan::virtual::next", level = "trace", skip_all)]
	async fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(self.context.is_some(), "VirtualScanNode::next() called before initialize()");
		self.virtual_table.next(rx).await
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		Some(self.headers.clone())
	}
}
