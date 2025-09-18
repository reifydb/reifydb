// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of table scan operations

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	flow::{FlowNodeDef, FlowNodeType},
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

		// Get namespace information
		let namespace_def = CatalogStore::get_namespace(unsafe { &mut *compiler.txn }, table.namespace)?;

		let namespace = FlowNodeDef::new(
			table.columns.clone(),
			Some(namespace_def.name.clone()),
			Some(table.name.clone()),
		);

		compiler.build_node(FlowNodeType::SourceTable {
			name: table_name,
			table: table.id,
			namespace,
		})
		.build()
	}
}
