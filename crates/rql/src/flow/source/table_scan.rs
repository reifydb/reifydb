// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Compilation of table scan operations

use FlowNodeType::SourceTable;
use reifydb_core::interface::{CommandTransaction, FlowNodeId};

use super::super::{CompileOperator, FlowCompiler, FlowNodeType};
use crate::{Result, plan::physical::TableScanNode};

pub(crate) struct TableScanCompiler<'a> {
	pub table_scan: TableScanNode<'a>,
}

impl<'a> From<TableScanNode<'a>> for TableScanCompiler<'a> {
	fn from(table_scan: TableScanNode<'a>) -> Self {
		Self {
			table_scan,
		}
	}
}

impl<'a, T: CommandTransaction> CompileOperator<T> for TableScanCompiler<'a> {
	async fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		compiler.build_node(SourceTable {
			table: self.table_scan.source.def().id,
		})
		.build()
		.await
	}
}
