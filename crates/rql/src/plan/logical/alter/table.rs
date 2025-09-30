// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
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
pub struct AlterTableNode<'a> {
	pub table: MaybeQualifiedTableIdentifier<'a>,
	pub operations: Vec<AlterTableOperation<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOperation<'a> {
	CreatePrimaryKey {
		name: Option<Fragment<'a>>,
		columns: Vec<AlterIndexColumn<'a>>,
	},
	DropPrimaryKey,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterIndexColumn<'a> {
	pub column: MaybeQualifiedColumnIdentifier<'a>,
	pub order: Option<SortDirection>,
}

impl Compiler {
	pub(crate) fn compile_alter_table<'a, T: CatalogQueryTransaction>(
		ast: AstAlterTable<'a>,
		_tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
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
