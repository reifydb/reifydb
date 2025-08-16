// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of table scan operations

use reifydb_core::interface::{FlowNodeId, Transaction};
use reifydb_rql::plan::physical::TableScanNode;

use crate::{
	FlowNodeType, Result,
	compile::{CompileOperator, FlowCompiler},
};

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

impl<T: Transaction> CompileOperator<T> for TableScanCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let table = self.table_scan.table;
		let table_name = table.name.clone();

		compiler.build_node(FlowNodeType::SourceTable {
			name: table_name,
			table: table.id,
		})
		.build()
	}
}
