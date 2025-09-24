// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::interface::evaluate::expression::{AliasExpression, IdentExpression};
use reifydb_type::{OwnedFragment, diagnostic::Diagnostic, err};

use crate::{
	ast::{Ast, AstFrom},
	expression::ExpressionCompiler,
	plan::logical::{Compiler, InlineDataNode, LogicalPlan, SourceScanNode, resolver},
};

impl Compiler {
	pub(crate) fn compile_from<'a, T: CatalogQueryTransaction>(
		ast: AstFrom<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		match ast {
			AstFrom::Source {
				source,
				..
			} => {
				// Use resolve to properly resolve
				// UnresolvedSourceIdentifier
				let resolved_source = resolver::resolve_unresolved_source(tx, &source)?;

				// TODO: Resolve index if present
				// For now, leave index as None
				let index = None;

				Ok(LogicalPlan::SourceScan(SourceScanNode {
					source: resolved_source,
					columns: None, /* Will be populated
					                * later when needed */
					index,
				}))
			}
			AstFrom::Inline {
				list,
				..
			} => {
				let mut rows = Vec::new();

				for row in list.nodes {
					match row {
						Ast::Inline(row) => {
							let mut alias_fields = Vec::new();
							for field in row.keyed_values {
								let key_fragment = field.key.token.fragment.clone();
								let alias = IdentExpression(key_fragment.clone());
								let expr = ExpressionCompiler::compile(
									field.value.as_ref().clone(),
								)?;

								let alias_expr = AliasExpression {
									alias,
									expression: Box::new(expr),
									fragment: key_fragment,
								};
								alias_fields.push(alias_expr);
							}
							rows.push(alias_fields);
						}
						_ => {
							return err!(Diagnostic {
								code: "E0001".to_string(),
								statement: None,
								message: "Expected row in static data".to_string(),
								column: None,
								fragment: OwnedFragment::None,
								label: None,
								help: None,
								notes: vec![],
								cause: None
							});
						}
					}
				}

				Ok(LogicalPlan::InlineData(InlineDataNode {
					rows,
				}))
			}
		}
	}
}
