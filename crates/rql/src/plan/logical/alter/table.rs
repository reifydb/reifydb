// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::SortDirection;
use reifydb_type::Fragment;

use crate::{
	ast::{
		AstAlterTable, AstAlterTableOperation,
		identifier::{MaybeQualifiedColumnIdentifier, MaybeQualifiedTableIdentifier},
	},
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableNode {
	pub table: MaybeQualifiedTableIdentifier,
	pub operations: Vec<AlterTableOperation>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOperation {
	CreatePrimaryKey {
		name: Option<Fragment>,
		columns: Vec<AlterIndexColumn>,
	},
	DropPrimaryKey,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterIndexColumn {
	pub column: MaybeQualifiedColumnIdentifier,
	pub order: Option<SortDirection>,
}

impl Compiler {
	pub(crate) fn compile_alter_table(&self, ast: AstAlterTable) -> crate::Result<LogicalPlan> {
		// Use the table identifier directly from AST
		let table = ast.table.clone();

		// Convert operations
		let operations = ast
			.operations
			.into_iter()
			.map(|op| {
				match op {
					AstAlterTableOperation::CreatePrimaryKey {
						name,
						columns,
					} => {
						// Convert columns to AlterIndexColumn
						let qualified_columns = columns
							.into_iter()
							.map(|col| AlterIndexColumn {
								column: col.column.clone(),
								order: col.order,
							})
							.collect();
						AlterTableOperation::CreatePrimaryKey {
							name,
							columns: qualified_columns,
						}
					}
					AstAlterTableOperation::DropPrimaryKey => AlterTableOperation::DropPrimaryKey,
				}
			})
			.collect();

		let node = AlterTableNode {
			table,
			operations,
		};
		Ok(LogicalPlan::AlterTable(node))
	}
}
