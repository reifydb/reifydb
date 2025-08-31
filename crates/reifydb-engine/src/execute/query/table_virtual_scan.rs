// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::{Params, Transaction};

use crate::{
	StandardTransaction,
	columnar::layout::ColumnsLayout,
	execute::{Batch, ExecutionContext, QueryNode},
	table_virtual::{TableVirtual, TableVirtualContext},
};

pub(crate) struct VirtualScanNode<'a, T: Transaction> {
	virtual_table: Box<dyn TableVirtual<'a, T>>,
	params: Params,
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
			params: context.params.clone(),
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
				params: self.params.clone(),
			}
		});
		self.virtual_table.initialize(rx, ctx)?;
		Ok(())
	}

	fn next(
		&mut self,
		rx: &mut StandardTransaction<'a, T>,
	) -> crate::Result<Option<Batch>> {
		self.virtual_table.next(rx)
	}

	fn layout(&self) -> Option<ColumnsLayout> {
		Some(self.layout.clone())
	}
}
