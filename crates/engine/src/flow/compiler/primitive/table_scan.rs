// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Compilation of table scan operations

use reifydb_core::{Result, interface::FlowNodeId};
use reifydb_rql::{flow::FlowNodeType::SourceTable, plan::physical::TableScanNode};

use super::super::{CompileOperator, FlowCompiler};
use crate::StandardCommandTransaction;

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

impl CompileOperator for TableScanCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut StandardCommandTransaction) -> Result<FlowNodeId> {
		let table_id = self.table_scan.source.def().id;
		compiler.add_node(
			txn,
			SourceTable {
				table: table_id,
			},
		)
	}
}
