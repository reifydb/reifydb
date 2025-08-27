// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogStore;
use reifydb_core::{
	SortDirection,
	interface::{QueryTransaction, SchemaDef, ViewDef},
	result::error::diagnostic::catalog::{
		schema_not_found, view_not_found,
	},
	return_error,
};

use crate::plan::{
	logical::alter::{
		AlterViewNode, AlterViewOperation as LogicalAlterViewOp,
	},
	physical::{Compiler, PhysicalPlan},
};

#[derive(Debug, Clone)]
pub struct AlterViewPlan {
	pub schema: SchemaDef,
	pub view: ViewDef,
	pub operations: Vec<AlterViewOperation>,
}

#[derive(Debug, Clone)]
pub enum AlterViewOperation {
	CreatePrimaryKey {
		name: Option<String>,
		columns: Vec<(String, SortDirection)>,
	},
	DropPrimaryKey,
}

impl Compiler {
	pub(crate) fn compile_alter_view(
		rx: &mut impl QueryTransaction,
		node: AlterViewNode,
	) -> crate::Result<PhysicalPlan> {
		// Resolve the schema
		let Some(schema_def) = CatalogStore::find_schema_by_name(
			rx,
			&node.schema.text(),
		)?
		else {
			return_error!(schema_not_found(
				&node.schema,
				node.schema.text()
			))
		};

		// Resolve the view
		let Some(view_def) = CatalogStore::find_view_by_name(
			rx,
			schema_def.id,
			&node.view.text(),
		)?
		else {
			return_error!(view_not_found(
				&node.schema,
				&node.schema.text(),
				&node.view.text()
			))
		};

		// Convert logical operations to physical operations
		let mut operations = Vec::new();
		for op in node.operations {
			match op {
				LogicalAlterViewOp::CreatePrimaryKey {
					name,
					columns,
				} => {
					operations.push(
						AlterViewOperation::CreatePrimaryKey {
							name,
							columns,
						},
					);
				}
				LogicalAlterViewOp::DropPrimaryKey => {
					operations
						.push(AlterViewOperation::DropPrimaryKey);
				}
			}
		}

		Ok(PhysicalPlan::AlterView(AlterViewPlan {
			schema: schema_def,
			view: view_def,
			operations,
		}))
	}
}
