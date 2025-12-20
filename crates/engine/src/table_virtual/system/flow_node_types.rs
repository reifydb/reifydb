// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::system::SystemCatalog;
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

/// Virtual table that exposes all FlowNodeType variants
pub struct FlowNodeTypes {
	definition: Arc<TableVirtualDef>,
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
	"merge",
	"sort",
	"take",
	"distinct",
	"apply",
	"sink_view",
	"window",
];

impl<'a> TableVirtual<'a> for FlowNodeTypes {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, _ctx: TableVirtualContext<'a>) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut StandardTransaction<'a>) -> Result<Option<Batch<'a>>> {
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
				name: Fragment::owned_internal("id"),
				data: ids,
			},
			Column {
				name: Fragment::owned_internal("name"),
				data: names,
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
