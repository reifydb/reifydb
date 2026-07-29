// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext, system::node_horizon_store::NodeHorizonStore},
};

pub struct SystemFlowNodes {
	pub(crate) vtable: Arc<VTable>,
	horizons: NodeHorizonStore,
	exhausted: bool,
}

impl SystemFlowNodes {
	pub fn new(horizons: NodeHorizonStore) -> Self {
		Self {
			vtable: SystemCatalog::get_system_flow_nodes_table().clone(),
			horizons,
			exhausted: false,
		}
	}
}

impl BaseVTable for SystemFlowNodes {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let nodes = CatalogStore::list_flow_nodes_all(txn)?;

		let mut ids = ColumnBuffer::uint8_with_capacity(nodes.len());
		let mut flow_ids = ColumnBuffer::uint8_with_capacity(nodes.len());
		let mut node_types = ColumnBuffer::uint1_with_capacity(nodes.len());
		let mut data_column = ColumnBuffer::blob_with_capacity(nodes.len());
		let mut stateful = ColumnBuffer::bool_with_capacity(nodes.len());
		let mut horizons = ColumnBuffer::utf8_with_capacity(nodes.len());
		let mut spans = ColumnBuffer::duration_with_capacity(nodes.len());

		for node in nodes {
			ids.push(node.id.0);
			flow_ids.push(node.flow.0);
			node_types.push(node.node_type);
			data_column.push(node.data);

			match self.horizons.get(node.id) {
				None => {
					stateful.push_none();
					horizons.push_none();
					spans.push_none();
				}
				Some(horizon) => {
					stateful.push(horizon.stateful);
					match horizon.span {
						None => {
							horizons.push("perpetual");
							spans.push_none();
						}
						Some(span) => {
							horizons.push("span");
							spans.push(span);
						}
					}
				}
			}
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("flow_id"), flow_ids),
			ColumnWithName::new(Fragment::internal("node_type"), node_types),
			ColumnWithName::new(Fragment::internal("data"), data_column),
			ColumnWithName::new(Fragment::internal("stateful"), stateful),
			ColumnWithName::new(Fragment::internal("horizon"), horizons),
			ColumnWithName::new(Fragment::internal("span"), spans),
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
