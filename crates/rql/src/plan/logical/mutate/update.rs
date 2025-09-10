// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::identifier::SourceIdentifier;
use reifydb_type::{Fragment, OwnedFragment};

use crate::{
	ast::AstUpdate,
	plan::logical::{Compiler, LogicalPlan, UpdateNode},
};

impl Compiler {
	pub(crate) fn compile_update<'a>(
		ast: AstUpdate<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		// Convert MaybeQualifiedSourceIdentifier to SourceIdentifier
		let target = ast.target.as_ref().map(|t| {
			let schema = t.schema.clone().unwrap_or_else(|| {
				Fragment::Owned(OwnedFragment::Internal {
					text: String::from("default"),
				})
			});
			SourceIdentifier::new(schema, t.name.clone(), t.kind)
		});

		Ok(LogicalPlan::Update(UpdateNode {
			target,
			input: None, /* Input will be set by the pipeline
			              * builder */
		}))
	}
}
