// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_rql::{flow::operator::OperatorDef::SourceTable, nodes::TableScanNode};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

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
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		let table = self.table_scan.source.def();
		let time_domain = table.time.domain();
		let table_id = table.id;
		compiler.add_node(
			txn,
			SourceTable {
				table: table_id,
				time_domain,
			},
		)
	}
}
