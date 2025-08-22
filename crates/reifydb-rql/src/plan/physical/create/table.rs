// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::plan::{
	logical::CreateTableNode,
	physical::{Compiler, CreateTablePlan, PhysicalPlan},
};
use reifydb_catalog::Catalog;
use reifydb_core::interface::UnderlyingQueryTransaction;
use reifydb_core::{
	diagnostic::catalog::schema_not_found
	, return_error,
};
use PhysicalPlan::CreateTable;

impl Compiler {
	pub(crate) fn compile_create_table(
		rx: &mut impl UnderlyingQueryTransaction,
		create: CreateTableNode,
	) -> crate::Result<PhysicalPlan> {
		let catalog = Catalog::new();
		let Some(schema) = catalog
			.find_schema_by_name(rx, &create.schema.fragment())?
		else {
			return_error!(schema_not_found(
				Some(create.schema.clone()),
				&create.schema.fragment()
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
