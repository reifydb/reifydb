// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::QueryTransaction;

use crate::plan::{
	logical::CreateSchemaNode,
	physical::{Compiler, CreateSchemaPlan, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_schema<'a>(
		_rx: &mut impl QueryTransaction,
		create: CreateSchemaNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		// FIXME validate catalog
		Ok(PhysicalPlan::CreateSchema(CreateSchemaPlan {
			schema: create.schema,
			if_not_exists: create.if_not_exists,
		}))
	}
}
