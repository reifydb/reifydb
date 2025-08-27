// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{OwnedFragment, SortDirection};

use crate::{
	ast::{AstAlterView, AstAlterViewOperation},
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterViewNode {
	pub schema: OwnedFragment,
	pub view: OwnedFragment,
	pub operations: Vec<AlterViewOperation>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterViewOperation {
	CreatePrimaryKey {
		name: Option<String>,
		columns: Vec<(String, SortDirection)>,
	},
	DropPrimaryKey,
}

impl Compiler {
	pub(super) fn compile_alter_view(
		node: AstAlterView,
	) -> crate::Result<LogicalPlan> {
		let mut operations = Vec::new();

		for op in node.operations {
			match op {
				AstAlterViewOperation::CreatePrimaryKey {
					name,
					columns,
				} => {
					let columns = columns
						.into_iter()
						.map(|c| {
							(
								c.column.value().to_string(),
								c.order.unwrap_or(
									SortDirection::Asc,
								),
							)
						})
						.collect();

					operations.push(
						AlterViewOperation::CreatePrimaryKey {
							name: name.map(|n| n.value().to_string()),
							columns,
						},
					);
				}
				AstAlterViewOperation::DropPrimaryKey => {
					operations.push(
						AlterViewOperation::DropPrimaryKey,
					);
				}
			}
		}

		Ok(LogicalPlan::AlterView(AlterViewNode {
			schema: node.schema.fragment(),
			view: node.view.fragment(),
			operations,
		}))
	}
}
