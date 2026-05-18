// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTable,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{BaseVTable, Batch, VTableContext},
};

pub struct SystemFlowNodeTypes {
	pub(crate) vtable: Arc<VTable>,
	exhausted: bool,
}

impl Default for SystemFlowNodeTypes {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemFlowNodeTypes {
	pub fn new() -> Self {
		Self {
			vtable: SystemCatalog::get_system_flow_node_types_table().clone(),
			exhausted: false,
		}
	}
}

const FLOW_NODE_TYPE_NAMES: [&str; 22] = [
	"source_inline_data",
	"source_table",
	"source_view",
	"source_flow",
	"filter",
	"map",
	"extend",
	"join",
	"aggregate",
	"append",
	"sort",
	"take",
	"distinct",
	"apply",
	"sink_subscription",
	"window",
	"source_ring_buffer",
	"source_series",
	"gate",
	"sink_table_view",
	"sink_ring_buffer_view",
	"sink_series_view",
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

		let mut ids = ColumnBuffer::uint1_with_capacity(FLOW_NODE_TYPE_NAMES.len());
		let mut names = ColumnBuffer::utf8_with_capacity(FLOW_NODE_TYPE_NAMES.len());

		for (i, name) in FLOW_NODE_TYPE_NAMES.iter().enumerate() {
			ids.push(i as u8);
			names.push(*name);
		}

		let columns = vec![
			ColumnWithName::new(Fragment::internal("id"), ids),
			ColumnWithName::new(Fragment::internal("name"), names),
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
