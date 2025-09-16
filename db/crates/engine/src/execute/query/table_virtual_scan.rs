// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::{
	interface::Transaction, value::columnar::layout::ColumnsLayout,
};

use crate::{
	StandardTransaction,
	execute::{Batch, ExecutionContext, QueryNode},
	table_virtual::{TableVirtual, TableVirtualContext},
};

pub(crate) struct VirtualScanNode<'a, T: Transaction> {
	virtual_table: Box<dyn TableVirtual<'a, T>>,
	context: Option<Arc<ExecutionContext>>,
	layout: ColumnsLayout,
	table_context: Option<TableVirtualContext<'a>>,
}

impl<'a, T: Transaction> VirtualScanNode<'a, T> {
	pub fn new(
		virtual_table: Box<dyn TableVirtual<'a, T>>,
		context: Arc<ExecutionContext>,
		table_context: TableVirtualContext<'a>,
	) -> crate::Result<Self> {
		let def = virtual_table.definition();

		let layout = ColumnsLayout {
			columns: def
				.columns
				.iter()
				.map(|col| {
					reifydb_core::value::columnar::layout::ColumnLayout {
						namespace: None,
						source: None,
						name: col.name.clone()}
				})
				.collect(),
		};

		Ok(Self {
			virtual_table,
			context: Some(context),
			layout,
			table_context: Some(table_context),
		})
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for VirtualScanNode<'a, T> {
	fn initialize(
		&mut self,
		rx: &mut StandardTransaction<'a, T>,
		_ctx: &ExecutionContext,
	) -> crate::Result<()> {
		let ctx = self.table_context.take().unwrap_or_else(|| {
			TableVirtualContext::Basic {
				params: self
					.context
					.as_ref()
					.unwrap()
					.params
					.clone(),
			}
		});
		self.virtual_table.initialize(rx, ctx)?;
		Ok(())
	}

	fn next(
		&mut self,
		rx: &mut StandardTransaction<'a, T>,
	) -> crate::Result<Option<Batch>> {
		debug_assert!(
			self.context.is_some(),
			"VirtualScanNode::next() called before initialize()"
		);
		self.virtual_table.next(rx)
	}

	fn layout(&self) -> Option<ColumnsLayout> {
		Some(self.layout.clone())
	}
}
