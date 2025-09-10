// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::AstInsert,
	plan::logical::{Compiler, InsertNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_insert<'a>(
		ast: AstInsert<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Convert MaybeQualified to fully qualified
		use reifydb_core::interface::identifier::SourceIdentifier;
		use reifydb_type::{Fragment, OwnedFragment};

		let schema = ast.target.schema.unwrap_or_else(|| {
			Fragment::Owned(OwnedFragment::Internal {
				text: String::from("default"),
			})
		});

		let mut target = SourceIdentifier::new(
			schema,
			ast.target.name,
			ast.target.kind,
		);
		if let Some(alias) = ast.target.alias {
			target = target.with_alias(alias);
		}

		Ok(LogicalPlan::Insert(InsertNode {
			target,
		}))
	}
}
