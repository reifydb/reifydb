// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::{CatalogStore, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::TableVirtualDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	StandardTransaction,
	execute::Batch,
	table_virtual::{TableVirtual, TableVirtualContext},
};

/// Virtual table that exposes system flow node information
pub struct FlowNodes {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
}

impl FlowNodes {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_flow_nodes_table_def().clone(),
			exhausted: false,
		}
	}
}

impl<'a> TableVirtual<'a> for FlowNodes {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		let nodes = CatalogStore::list_flow_nodes_all(txn)?;

		let mut ids = ColumnData::uint8_with_capacity(nodes.len());
		let mut flow_ids = ColumnData::uint8_with_capacity(nodes.len());
		let mut node_types = ColumnData::uint1_with_capacity(nodes.len());
		let mut data_column = ColumnData::blob_with_capacity(nodes.len());

		for node in nodes {
			ids.push(node.id.0);
			flow_ids.push(node.flow.0);
			node_types.push(node.node_type);
			data_column.push(node.data);
		}

		let columns = vec![
			Column {
				name: Fragment::owned_internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::owned_internal("flow_id"),
				data: flow_ids,
			},
			Column {
				name: Fragment::owned_internal("node_type"),
				data: node_types,
			},
			Column {
				name: Fragment::owned_internal("data"),
				data: data_column,
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}
