// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_type::{err, error::diagnostic::Diagnostic, fragment::Fragment};

use crate::{
	ast::ast::{Ast, AstFrom},
	expression::{AliasExpression, ExpressionCompiler, IdentExpression},
	plan::logical::{
		Compiler, EnvironmentNode, GeneratorNode, InlineDataNode, LogicalPlan, PrimitiveScanNode,
		VariableSourceNode, resolver,
	},
};

impl Compiler {
	pub(crate) fn compile_from<T: IntoStandardTransaction>(
		&self,
		ast: AstFrom,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
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
				let variable_name = variable.token.fragment.clone();
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
