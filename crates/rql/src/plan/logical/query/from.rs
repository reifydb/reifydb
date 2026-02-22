// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;
use reifydb_type::{err, error::Diagnostic, fragment::Fragment};

use crate::{
	ast::ast::{Ast, AstFrom},
	bump::BumpBox,
	expression::{AliasExpression, ExpressionCompiler, IdentExpression},
	plan::logical::{
		Compiler, EnvironmentNode, GeneratorNode, InlineDataNode, LogicalPlan, PrimitiveScanNode,
		VariableSourceNode, resolver,
	},
};

// Note: Fragment is still imported for use at materialization boundaries (Expression types use owned Fragment)

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_from(
		&self,
		ast: AstFrom<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		match ast {
			AstFrom::Source {
				source,
				..
			} => {
				let resolved_source = resolver::resolve_unresolved_source(&self.catalog, tx, &source)?;

				Ok(LogicalPlan::PrimitiveScan(PrimitiveScanNode {
					source: resolved_source,
					columns: None,
					index: None,
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
								let key_fragment = field.key.token.fragment.to_owned();
								let alias = IdentExpression(key_fragment.clone());
								let expr = ExpressionCompiler::compile(
									BumpBox::into_inner(field.value),
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
								message: "Expected encoded in static data".to_string(),
								column: None,
								fragment: Fragment::None,
								label: None,
								help: None,
								notes: vec![],
								cause: None,
								operator_chain: None,
							});
						}
					}
				}

				Ok(LogicalPlan::InlineData(InlineDataNode {
					rows,
				}))
			}
			AstFrom::Generator(generator) => {
				let expressions = generator
					.nodes
					.into_iter()
					.map(ExpressionCompiler::compile)
					.collect::<crate::Result<Vec<_>>>()?;

				Ok(LogicalPlan::Generator(GeneratorNode {
					name: generator.name,
					expressions,
				}))
			}
			AstFrom::Variable {
				variable,
				..
			} => {
				// Create a variable source node for regular variables
				let variable_name = variable.token.fragment;
				Ok(LogicalPlan::VariableSource(VariableSourceNode {
					name: variable_name,
				}))
			}

			AstFrom::Environment {
				..
			} => Ok(LogicalPlan::Environment(EnvironmentNode {})),
		}
	}
}
