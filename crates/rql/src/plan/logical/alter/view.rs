// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::{
	SortDirection,
	interface::identifier::{ColumnIdentifier, SourceIdentifier},
};
use reifydb_type::Fragment;

use crate::{
	ast::{AstAlterView, AstAlterViewOperation},
	plan::logical::{Compiler, LogicalPlan, resolver::IdentifierResolver},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterViewNode<'a> {
	pub view: SourceIdentifier<'a>,
	pub operations: Vec<AlterViewOperation<'a>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterViewOperation<'a> {
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
	pub(crate) fn compile_alter_view<'a, 't, T: CatalogQueryTransaction>(
		ast: AstAlterView<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Resolve the view identifier
		let view = resolver.resolve_maybe_source(&ast.view)?;

		// Convert operations
		let operations = ast
			.operations
			.into_iter()
			.map(|op| {
				match op {
                AstAlterViewOperation::CreatePrimaryKey { name, columns } => {
                    // Convert columns to use qualified identifiers
                    let qualified_columns = columns.into_iter().map(|col| {
                        use reifydb_core::interface::identifier::ColumnSource;

                        AlterIndexColumn {
                            column: ColumnIdentifier {
                                source: ColumnSource::Source {
                                    schema: view.schema.clone(),
                                    source: view.name.clone(),
                                },
                                name: col.column.name,
                            },
                            order: col.order,
                        }
                    }).collect();

                    AlterViewOperation::CreatePrimaryKey {
                        name,
                        columns: qualified_columns,
                    }
                }
                AstAlterViewOperation::DropPrimaryKey => {
                    AlterViewOperation::DropPrimaryKey
                }
            }
			})
			.collect();

		let node = AlterViewNode {
			view,
			operations,
		};
		Ok(LogicalPlan::AlterView(node))
	}
}
