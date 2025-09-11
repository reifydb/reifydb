// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of table scan operations

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	flow::{FlowNodeSchema, FlowNodeType},
	interface::{CommandTransaction, FlowNodeId},
};

use super::super::{CompileOperator, FlowCompiler};
use crate::{Result, plan::physical::TableScanNode};

pub(crate) struct TableScanCompiler {
	pub table_scan: TableScanNode,
}

impl From<TableScanNode> for TableScanCompiler {
	fn from(table_scan: TableScanNode) -> Self {
		Self {
			table_scan,
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for TableScanCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let table = self.table_scan.table;
		let table_name = table.name.clone();

		// Get schema information
		let schema_def = CatalogStore::get_schema(
			unsafe { &mut *compiler.txn },
			table.schema,
		)?;

		let schema = FlowNodeSchema::new(
			table.columns.clone(),
			Some(schema_def.name.clone()),
			Some(table.name.clone()),
		);

		compiler.build_node(FlowNodeType::SourceTable {
			name: table_name,
			table: table.id,
			schema,
		})
		.build()
	}
}
