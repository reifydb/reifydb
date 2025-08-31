// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::Transaction;

use crate::{
	StandardTransaction,
	columnar::layout::ColumnsLayout,
	execute::{Batch, ExecutionContext},
	table_virtual::{VirtualTable, VirtualTableContext},
};

pub(crate) struct VirtualScanNode<T: Transaction> {
	virtual_table: Box<dyn VirtualTable<T>>,
	context: Arc<ExecutionContext>,
	layout: ColumnsLayout,
	initialized: bool,
	table_context: Option<VirtualTableContext<'static>>,
}

impl<T: Transaction> VirtualScanNode<T> {
	pub fn new(
		virtual_table: Box<dyn VirtualTable<T>>,
		context: Arc<ExecutionContext>,
		table_context: VirtualTableContext<'static>,
	) -> crate::Result<Self> {
		let def = virtual_table.definition();

		let layout = ColumnsLayout {
			columns: def
				.columns
				.iter()
				.map(|col| {
					crate::columnar::layout::ColumnLayout {
						schema: None,
						source: None,
						name: col.name.clone(),
					}
				})
				.collect(),
		};

		Ok(Self {
			virtual_table,
			context,
			layout,
			initialized: false,
			table_context: Some(table_context),
		})
	}
}

impl<T: Transaction> VirtualScanNode<T> {
	pub(crate) fn next(
		&mut self,
		_ctx: &ExecutionContext,
		rx: &mut StandardTransaction<T>,
	) -> crate::Result<Option<Batch>> {
		// Initialize on first call
		if !self.initialized {
			let ctx = self.table_context.take().unwrap_or_else(
				|| VirtualTableContext::Basic {
					params: self.context.params.clone(),
				},
			);

			self.virtual_table.initialize(rx, ctx)?;
			self.initialized = true;
		}

		// Delegate to virtual table's iterator
		self.virtual_table.next(rx)
	}

	pub(crate) fn layout(&self) -> Option<ColumnsLayout> {
		Some(self.layout.clone())
	}
}
