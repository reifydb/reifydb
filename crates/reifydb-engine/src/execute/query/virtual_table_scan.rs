// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::Transaction;

use crate::{
	StandardTransaction,
	columnar::layout::ColumnsLayout,
	execute::{Batch, ExecutionContext},
	table_virtual::{VirtualTable, VirtualTableQueryContext},
};

pub(crate) struct VirtualScanNode<T: Transaction> {
	virtual_table: Box<dyn VirtualTable<T>>,
	context: Arc<ExecutionContext>,
	layout: ColumnsLayout,
	exhausted: bool,
}

impl<T: Transaction> VirtualScanNode<T> {
	pub fn new(
		virtual_table: Box<dyn VirtualTable<T>>,
		context: Arc<ExecutionContext>,
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
			exhausted: false,
		})
	}
}

impl<T: Transaction> VirtualScanNode<T> {
	pub(crate) fn next(
		&mut self,
		_ctx: &ExecutionContext,
		rx: &mut StandardTransaction<T>,
	) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		// Build the query context for pushdown operations
		// TODO: Extract these from the query plan in the future
		let query_ctx = VirtualTableQueryContext {
			filters: Vec::new(),
			projections: Vec::new(),
			order_by: Vec::new(),
			limit: None,
			params: self.context.params.clone(),
		};

		// Execute the virtual table query
		let columns = self.virtual_table.query(query_ctx, rx)?;

		self.exhausted = true; // For now, virtual tables return all data at once

		Ok(Some(Batch {
			columns,
		}))
	}

	pub(crate) fn layout(&self) -> Option<ColumnsLayout> {
		Some(self.layout.clone())
	}
}
