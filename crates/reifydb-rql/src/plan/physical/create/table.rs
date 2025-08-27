// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use PhysicalPlan::CreateTable;
use reifydb_catalog::CatalogStore;
use reifydb_core::{
	diagnostic::catalog::schema_not_found, interface::QueryTransaction,
	return_error,
};

use crate::plan::{
	logical::CreateTableNode,
	physical::{Compiler, CreateTablePlan, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_table(
		rx: &mut impl QueryTransaction,
		create: CreateTableNode,
	) -> crate::Result<PhysicalPlan> {
		let Some(schema) = CatalogStore::find_schema_by_name(
			rx,
			&create.schema.text(),
		)?
		else {
			return_error!(schema_not_found(
				Some(create.schema.clone()),
				&create.schema.text()
			));
		};

		Ok(CreateTable(CreateTablePlan {
			schema,
			table: create.table,
			if_not_exists: create.if_not_exists,
			columns: create.columns,
		}))
	}
}
