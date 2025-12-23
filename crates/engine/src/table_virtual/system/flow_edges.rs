// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
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

/// Virtual table that exposes system flow edge information
pub struct FlowEdges {
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
}

impl FlowEdges {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_flow_edges_table_def().clone(),
			exhausted: false,
		}
	}
}

#[async_trait]
impl TableVirtual for FlowEdges {
	async fn initialize<'a>(
		&mut self,
		_txn: &mut StandardTransaction<'a>,
		_ctx: TableVirtualContext,
	) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next<'a>(&mut self, txn: &mut StandardTransaction<'a>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let edges = CatalogStore::list_flow_edges_all(txn).await?;

		let mut ids = ColumnData::uint8_with_capacity(edges.len());
		let mut flow_ids = ColumnData::uint8_with_capacity(edges.len());
		let mut sources = ColumnData::uint8_with_capacity(edges.len());
		let mut targets = ColumnData::uint8_with_capacity(edges.len());

		for edge in edges {
			ids.push(edge.id.0);
			flow_ids.push(edge.flow.0);
			sources.push(edge.source.0);
			targets.push(edge.target.0);
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
				name: Fragment::internal("source"),
				data: sources,
			},
			Column {
				name: Fragment::internal("target"),
				data: targets,
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
