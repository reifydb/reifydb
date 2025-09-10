// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::identifier::SourceIdentifier;
use reifydb_type::{Fragment, OwnedFragment};

use crate::{
	ast::AstDelete,
	plan::logical::{Compiler, DeleteNode, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_delete<'a>(
		ast: AstDelete<'a>,
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

		Ok(LogicalPlan::Delete(DeleteNode {
			target,
			input: None, /* Input will be set by the pipeline
			              * builder */
		}))
	}
}
