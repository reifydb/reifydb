// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, builder::ColumnBuilder, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemFlowEdges {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemFlowEdges {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemFlowEdges {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_flow_edges_table().clone(),
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemFlowEdges {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let edges = CatalogStore::list_flow_edges_all(txn)?;

		let mut ids = ColumnBuilder::with_capacity(ValueType::Uint8, edges.len());
		let mut flow_ids = ColumnBuilder::with_capacity(ValueType::Uint8, edges.len());
		let mut sources = ColumnBuilder::with_capacity(ValueType::Uint8, edges.len());
		let mut targets = ColumnBuilder::with_capacity(ValueType::Uint8, edges.len());

		for edge in edges {
			ids.push(edge.id.0);
			flow_ids.push(edge.flow.0);
			sources.push(edge.source.0);
			targets.push(edge.target.0);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids.finish()),
			ColumnWithName::new(Fragment::internal("flow_id"), flow_ids.finish()),
			ColumnWithName::new(Fragment::internal("source"), sources.finish()),
			ColumnWithName::new(Fragment::internal("target"), targets.finish()),
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}
