// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{OwnedFragment, SortDirection};

use crate::{
	ast::{AstAlterTable, AstAlterTableOperation},
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableNode {
	pub schema: OwnedFragment,
	pub table: OwnedFragment,
	pub operations: Vec<AlterTableOperation>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOperation {
	CreatePrimaryKey {
		name: Option<String>,
		columns: Vec<(String, SortDirection)>,
	},
	DropPrimaryKey,
}

impl Compiler {
	pub(super) fn compile_alter_table(
		node: AstAlterTable,
	) -> crate::Result<LogicalPlan> {
		let mut operations = Vec::new();

		for op in node.operations {
			match op {
				AstAlterTableOperation::CreatePrimaryKey {
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
						AlterTableOperation::CreatePrimaryKey {
							name: name.map(|n| n.value().to_string()),
							columns,
						},
					);
				}
				AstAlterTableOperation::DropPrimaryKey => {
					operations.push(
						AlterTableOperation::DropPrimaryKey,
					);
				}
			}
		}

		Ok(LogicalPlan::AlterTable(AlterTableNode {
			schema: node.schema.fragment(),
			table: node.table.fragment(),
			operations,
		}))
	}
}
