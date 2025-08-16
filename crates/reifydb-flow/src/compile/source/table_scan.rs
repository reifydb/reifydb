// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of table scan operations

use reifydb_catalog::Catalog;
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
		let table_name = self.table_scan.table.fragment.clone();

		let table = compiler
			.txn
			.with_versioned_query(|rx| {
				Catalog::get_table_by_name(
					rx,
					self.table_scan.schema.id,
					&table_name,
				)
			})?
			.unwrap();

		compiler.build_node(FlowNodeType::SourceTable {
			name: table_name,
			table: table.id,
		})
		.build()
	}
}
