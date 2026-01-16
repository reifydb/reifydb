// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes system flow node information
pub struct FlowNodes {
	pub(crate) definition: Arc<VTableDef>,
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

impl<T: IntoStandardTransaction> VTable<T> for FlowNodes {
	fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
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
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("flow_id"),
				data: flow_ids,
			},
			Column {
				name: Fragment::internal("node_type"),
				data: node_types,
			},
			Column {
				name: Fragment::internal("data"),
				data: data_column,
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
