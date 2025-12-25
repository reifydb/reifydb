// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

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
	async fn compile(
		self,
		compiler: &mut FlowCompiler,
		txn: &mut StandardCommandTransaction,
	) -> Result<FlowNodeId> {
		compiler.add_node(
			txn,
			SourceTable {
				table: self.table_scan.source.def().id,
			},
		)
		.await
	}
}
