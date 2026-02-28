// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::VTableDef,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use crate::{
	Result,
	system::SystemCatalog,
	vtable::{Batch, VTable, VTableContext},
};

/// Virtual table that exposes all FlowNodeType variants
pub struct FlowNodeTypes {
	pub(crate) definition: Arc<VTableDef>,
	exhausted: bool,
}

impl FlowNodeTypes {
	pub fn new() -> Self {
		Self {
			definition: SystemCatalog::get_system_flow_node_types_table_def().clone(),
			exhausted: false,
		}
	}
}

/// FlowNodeType variant names in order of their discriminator values
const FLOW_NODE_TYPE_NAMES: [&str; 16] = [
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
	"sink_view",
	"window",
];

impl VTable for FlowNodeTypes {
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

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}
