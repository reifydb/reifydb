// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::SortDirection;
use reifydb_type::Fragment;

use crate::{
	ast::{
		AstAlterView, AstAlterViewOperation,
		identifier::{MaybeQualifiedColumnIdentifier, MaybeQualifiedViewIdentifier},
	},
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterViewNode {
	pub view: MaybeQualifiedViewIdentifier,
	pub operations: Vec<AlterViewOperation>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterViewOperation {
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
	pub(crate) fn compile_alter_view(&self, ast: AstAlterView) -> crate::Result<LogicalPlan> {
		// Use the view identifier directly from AST
		let view = ast.view.clone();

		// Convert operations
		let operations = ast
			.operations
			.into_iter()
			.map(|op| {
				match op {
					AstAlterViewOperation::CreatePrimaryKey {
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

						AlterViewOperation::CreatePrimaryKey {
							name,
							columns: qualified_columns,
						}
					}
					AstAlterViewOperation::DropPrimaryKey => AlterViewOperation::DropPrimaryKey,
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
