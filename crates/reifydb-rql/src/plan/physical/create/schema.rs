// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::UnderlyingQueryTransaction;

use crate::plan::{
	logical::CreateSchemaNode,
	physical::{Compiler, CreateSchemaPlan, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_schema(
		_rx: &mut impl UnderlyingQueryTransaction,
		create: CreateSchemaNode,
	) -> crate::Result<PhysicalPlan> {
		// FIXME validate catalog
		Ok(PhysicalPlan::CreateSchema(CreateSchemaPlan {
			schema: create.schema,
			if_not_exists: create.if_not_exists,
		}))
	}
}
