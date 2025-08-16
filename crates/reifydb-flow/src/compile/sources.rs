// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of data source logical plans to FlowGraph nodes

use reifydb_catalog::{Catalog, sequence::flow::next_flow_node_id};
use reifydb_core::interface::{
	ActiveCommandTransaction, FlowNodeId, Transaction,
};

use super::FlowCompiler;
use crate::{FlowNode, FlowNodeType};

impl FlowCompiler {
	pub(crate) fn compile_table_scan<T: Transaction>(
		&mut self,
		txn: &mut ActiveCommandTransaction<T>,
		table_scan: reifydb_rql::plan::physical::TableScanNode,
	) -> crate::Result<FlowNodeId> {
		// Process physical plan TableScanNode directly
		let table_name = table_scan.table.fragment.clone();

		let table = if let Some(schema_span) = table_scan.schema {
			let schema_name = schema_span.fragment;
			txn.with_versioned_query(|rx| {
				let schema = Catalog::get_schema_by_name(
					rx,
					&schema_name,
				)?
				.unwrap();
				Catalog::get_table_by_name(
					rx,
					schema.id,
					&table_name,
				)
			})?
			.unwrap()
		} else {
			// Use default schema if not specified
			txn.with_versioned_query(|rx| {
				let schema = Catalog::get_schema_by_name(
					rx, "public",
				)?
				.unwrap();
				Catalog::get_table_by_name(
					rx,
					schema.id,
					&table_name,
				)
			})?
			.unwrap()
		};

		// Create Source node for the table
		let node_id = self.flow.add_node(FlowNode::new(
			next_flow_node_id(txn)?,
			FlowNodeType::SourceTable {
				name: table_name,
				table: table.id,
			},
		));

		Ok(node_id)
	}

	pub(crate) fn compile_inline_data(
		&mut self,
		_inline_data: reifydb_rql::plan::physical::InlineDataNode,
	) -> crate::Result<FlowNodeId> {
		unimplemented!()
	}
}
