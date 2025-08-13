// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::VersionedQueryTransaction;

use crate::plan::{
	logical::CreateTableNode,
	physical::{Compiler, CreateTablePlan, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_table(
		_rx: &mut impl VersionedQueryTransaction,
		create: CreateTableNode,
	) -> crate::Result<PhysicalPlan> {
		// FIXME validate with catalog
		Ok(PhysicalPlan::CreateTable(CreateTablePlan {
			schema: create.schema,
			table: create.table,
			if_not_exists: create.if_not_exists,
			columns: create.columns,
		}))
	}
}
