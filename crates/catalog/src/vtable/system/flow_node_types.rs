// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

/// Virtual table that exposes all FlowNodeType variants
pub struct SystemFlowNodeTypes {
	pub(crate) definition: Arc<VTable>,
	exhausted: bool,
}

impl SystemFlowNodeTypes {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_flow_node_types_table().clone(),
			exhausted: false,
		}
	}
}

/// FlowNodeType variant names indexed by their discriminator values.
/// Discriminators are sparse (0-22 with a gap at 14), so we use an array
/// covering the full range with an empty string for the unused slot.
const FLOW_NODE_TYPE_NAMES: [&str; 22] = [
	"source_inline_data",    // 0
	"source_table",          // 1
	"source_view",           // 2
	"source_flow",           // 3
	"filter",                // 4
	"map",                   // 5
	"extend",                // 6
	"join",                  // 7
	"aggregate",             // 8
	"append",                // 9
	"sort",                  // 10
	"take",                  // 11
	"distinct",              // 12
	"apply",                 // 13
	"sink_subscription",     // 14
	"window",                // 15
	"source_ring_buffer",    // 16
	"source_series",         // 17
	"gate",                  // 18
	"sink_table_view",       // 19
	"sink_ring_buffer_view", // 20
	"sink_series_view",      // 21
];

impl BaseVTable for SystemFlowNodeTypes {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		let mut ids = ColumnData::uint1_with_capacity(FLOW_NODE_TYPE_NAMES.len());
		let mut names = ColumnData::utf8_with_capacity(FLOW_NODE_TYPE_NAMES.len());

		for (i, name) in FLOW_NODE_TYPE_NAMES.iter().enumerate() {
			ids.push(i as u8);
			names.push(*name);
		}

		let columns = vec![
			Column {
				name: Fragment::internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::internal("name"),
				data: names,
			},
		];

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &VTable {
		&self.definition
	}
}
