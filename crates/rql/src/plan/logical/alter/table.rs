// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::{
	SortDirection,
	interface::identifier::{ColumnIdentifier, TableIdentifier},
};
use reifydb_type::Fragment;

use crate::{
	ast::{AstAlterTable, AstAlterTableOperation},
	plan::logical::{Compiler, LogicalPlan, resolver::IdentifierResolver},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableNode<'a> {
	pub table: TableIdentifier<'a>,
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
	pub column: ColumnIdentifier<'a>,
	pub order: Option<SortDirection>,
}

impl Compiler {
	pub(crate) fn compile_alter_table<
		'a,
		't,
		T: CatalogQueryTransaction,
	>(
		ast: AstAlterTable<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Resolve the table identifier
		let table = resolver.resolve_table(&ast.table, true)?;

		// Convert operations
		let operations = ast
            .operations
            .into_iter()
            .map(|op| {
                match op {
                    AstAlterTableOperation::CreatePrimaryKey { name, columns } => {
                        // Convert columns to use qualified identifiers
                        let qualified_columns = columns.into_iter().map(|col| {
                            use reifydb_core::interface::identifier::ColumnSource;
                            AlterIndexColumn {
                                column: ColumnIdentifier {
                                    source: ColumnSource::Source {
                                        namespace: table.namespace.clone(),
                                        source: table.name.clone(),
                                    },
                                    name: col.column.name,
                                },
                                order: col.order,
                            }
                        }).collect();
						AlterTableOperation::CreatePrimaryKey {
                            name,
                            columns: qualified_columns,
                        }
                    }
                    AstAlterTableOperation::DropPrimaryKey => {
                        AlterTableOperation::DropPrimaryKey
                    }
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
