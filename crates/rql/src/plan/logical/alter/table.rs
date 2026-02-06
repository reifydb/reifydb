// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::sort::SortDirection;

use crate::{
	ast::{
		ast::{AstAlterTable, AstAlterTableOperation},
		identifier::{MaybeQualifiedColumnIdentifier, MaybeQualifiedTableIdentifier},
	},
	bump::BumpFragment,
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug)]
pub struct AlterTableNode<'bump> {
	pub table: MaybeQualifiedTableIdentifier<'bump>,
	pub operations: Vec<AlterTableOperation<'bump>>,
}

#[derive(Debug)]
pub enum AlterTableOperation<'bump> {
	CreatePrimaryKey {
		name: Option<BumpFragment<'bump>>,
		columns: Vec<AlterIndexColumn<'bump>>,
	},
	DropPrimaryKey,
}

#[derive(Debug)]
pub struct AlterIndexColumn<'bump> {
	pub column: MaybeQualifiedColumnIdentifier<'bump>,
	pub order: Option<SortDirection>,
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter_table(&self, ast: AstAlterTable<'bump>) -> crate::Result<LogicalPlan<'bump>> {
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
