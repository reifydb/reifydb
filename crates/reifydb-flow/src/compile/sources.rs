// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of data source logical plans to FlowGraph nodes

use reifydb_core::interface::{SchemaId, TableDef};
use reifydb_rql::plan::logical::{InlineDataNode, TableScanNode};

use super::FlowCompiler;
use crate::{Flow, NodeId, NodeType, Result};

impl FlowCompiler {
	/// Compiles a TableScan logical plan into a Source node
	pub(super) fn compile_table_scan(
		&mut self,
		flow_graph: &mut Flow,
		table_scan: TableScanNode,
	) -> Result<NodeId> {
		// Extract schema and table information
		let schema_id = if let Some(_schema_span) = table_scan.schema {
			// TODO: Resolve schema name to SchemaId through catalog
			SchemaId(1) // Placeholder
		} else {
			self.schema_context.unwrap_or(SchemaId(1))
		};

		let table_name = table_scan.table.fragment;
		let table_id = self.next_table_id();

		// Create table metadata
		let table = TableDef {
			id: table_id,
			schema: schema_id,
			name: table_name.clone(),
			columns: vec![], // TODO: Resolve columns from catalog
		};

		// Create Source node
		let node_id = flow_graph.add_node(NodeType::Source {
			name: table_name,
			table,
		});

		Ok(node_id)
	}

	/// Compiles an InlineData logical plan into a Source node with static
	/// data
	pub(super) fn compile_inline_data(
		&mut self,
		flow_graph: &mut Flow,
		_inline_data: InlineDataNode,
	) -> Result<NodeId> {
		let table_id = self.next_table_id();
		let schema_id = self.schema_context.unwrap_or(SchemaId(1));

		// Create table metadata for inline data
		let table = TableDef {
			id: table_id,
			schema: schema_id,
			name: format!("inline_data_{}", table_id.0),
			columns: vec![], /* TODO: Infer columns from inline
			                  * data structure */
		};

		// Create Source node
		// Note: The actual inline data will need to be stored and
		// emitted by the source This would require extending the
		// Source node type to handle static data
		let node_id = flow_graph.add_node(NodeType::Source {
			name: format!("inline_data_{}", table_id.0),
			table,
		});

		// TODO: Store the inline data rows for later emission
		// This might require extending NodeType::Source with optional
		// static data

		Ok(node_id)
	}
}
