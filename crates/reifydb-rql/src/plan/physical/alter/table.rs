// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::{
	SortDirection,
	interface::{QueryTransaction, SchemaDef, TableDef},
	result::error::diagnostic::catalog::{
		schema_not_found, table_not_found,
	},
	return_error,
};

use crate::plan::{
	logical::alter::{
		AlterTableNode, AlterTableOperation as LogicalAlterTableOp,
	},
	physical::{Compiler, PhysicalPlan},
};

#[derive(Debug, Clone)]
pub struct AlterTablePlan {
	pub schema: SchemaDef,
	pub table: TableDef,
	pub operations: Vec<AlterTableOperation>,
}

#[derive(Debug, Clone)]
pub enum AlterTableOperation {
	CreatePrimaryKey {
		name: Option<String>,
		columns: Vec<(String, SortDirection)>,
	},
	DropPrimaryKey,
}

impl Compiler {
	pub(crate) fn compile_alter_table<T>(
		rx: &mut T,
		node: AlterTableNode,
	) -> crate::Result<PhysicalPlan>
	where
		T: QueryTransaction + CatalogQueryTransaction,
	{
		// Resolve the schema
		let Some(schema_def) =
			rx.find_schema_by_name(&node.schema.text())?
		else {
			return_error!(schema_not_found(
				&node.schema,
				node.schema.text()
			))
		};

		// Resolve the table
		let Some(table_def) = rx.find_table_by_name(
			schema_def.id,
			&node.table.text(),
		)?
		else {
			return_error!(table_not_found(
				&node.schema,
				&node.schema.text(),
				&node.table.text()
			))
		};

		// Convert logical operations to physical operations
		let mut operations = Vec::new();
		for op in node.operations {
			match op {
				LogicalAlterTableOp::CreatePrimaryKey {
					name,
					columns,
				} => {
					operations.push(
						AlterTableOperation::CreatePrimaryKey {
							name,
							columns,
						},
					);
				}
				LogicalAlterTableOp::DropPrimaryKey => {
					operations
						.push(AlterTableOperation::DropPrimaryKey);
				}
			}
		}

		Ok(PhysicalPlan::AlterTable(AlterTablePlan {
			schema: schema_def,
			table: table_def,
			operations,
		}))
	}
}
