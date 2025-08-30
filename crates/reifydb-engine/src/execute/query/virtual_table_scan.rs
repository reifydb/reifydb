// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::{
	QueryTransaction, virtual_table::VirtualTableDef,
};

use crate::{
	columnar::layout::ColumnsLayout,
	execute::{Batch, ExecutionContext},
	virtual_table::{
		VirtualTable, VirtualTableQueryContext, system::Sequences,
	},
};

/// Enum of all available virtual system table implementations
pub(crate) enum VirtualSystemTables {
	Sequences(Sequences),
	// Future: Tables(TablesTable),
	// Future: Columns(ColumnsTable),
}

impl VirtualSystemTables {
	fn definition(&self) -> &VirtualTableDef {
		match self {
			VirtualSystemTables::Sequences(table) => {
				table.definition()
			}
		}
	}

	fn query<T: QueryTransaction>(
		&self,
		ctx: VirtualTableQueryContext,
	) -> crate::Result<crate::columnar::Columns> {
		match self {
			VirtualSystemTables::Sequences(table) => {
				table.query(ctx)
			}
		}
	}
}

pub(crate) struct VirtualScanNode {
	virtual_table: Box<dyn VirtualTable>,
	context: Arc<ExecutionContext>,
	layout: ColumnsLayout,
	exhausted: bool,
}

impl VirtualScanNode {
	pub fn new(
		virtual_table: Box<dyn VirtualTable>,
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

impl VirtualScanNode {
	pub(crate) fn next(
		&mut self,
		_ctx: &ExecutionContext,
		rx: &mut impl QueryTransaction,
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
		let columns = self.virtual_table.query(query_ctx)?;

		self.exhausted = true; // For now, virtual tables return all data at once

		Ok(Some(Batch {
			columns,
		}))
	}

	pub(crate) fn layout(&self) -> Option<ColumnsLayout> {
		Some(self.layout.clone())
	}
}
