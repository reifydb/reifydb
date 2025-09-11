// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstCreateSchema,
	plan::logical::{
		Compiler, CreateSchemaNode, LogicalPlan,
		resolver::IdentifierResolver,
	},
};

impl Compiler {
	pub(crate) fn compile_create_schema<
		'a,
		't,
		T: CatalogQueryTransaction,
	>(
		ast: AstCreateSchema<'a>,
		_resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Convert MaybeQualified to fully qualified
		use reifydb_core::interface::identifier::SchemaIdentifier;

		let schema = SchemaIdentifier::new(ast.schema.name);

		Ok(LogicalPlan::CreateSchema(CreateSchemaNode {
			schema,
			if_not_exists: false,
		}))
	}
}
