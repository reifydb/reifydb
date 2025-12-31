// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::{Batch, QueryTransaction, VTableDef},
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::Fragment;

use crate::{
	CatalogStore,
	system::SystemCatalog,
	vtable::{VTable, VTableContext},
};

/// Virtual table that exposes system flow edge information
pub struct FlowEdges {
	pub(crate) definition: Arc<VTableDef>,
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
impl<T: QueryTransaction> VTable<T> for FlowEdges {
	async fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>> {
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

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
