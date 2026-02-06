// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::sort::SortDirection;

use crate::{
	ast::{
		ast::{AstAlterView, AstAlterViewOperation},
		identifier::{MaybeQualifiedColumnIdentifier, MaybeQualifiedViewIdentifier},
	},
	bump::BumpFragment,
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug)]
pub struct AlterViewNode<'bump> {
	pub view: MaybeQualifiedViewIdentifier<'bump>,
	pub operations: Vec<AlterViewOperation<'bump>>,
}

#[derive(Debug)]
pub enum AlterViewOperation<'bump> {
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
	pub(crate) fn compile_alter_view(&self, ast: AstAlterView<'bump>) -> crate::Result<LogicalPlan<'bump>> {
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
