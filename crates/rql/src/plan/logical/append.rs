// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;
use reifydb_type::{err, error::diagnostic::Diagnostic, fragment::Fragment};

use crate::{
	ast::ast::{Ast, AstAppend, AstAppendSource, AstList},
	bump::BumpBox,
	expression::{AliasExpression, ExpressionCompiler, IdentExpression},
	plan::logical::{AppendNode, AppendSourcePlan, Compiler, InlineDataNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_append(
		&self,
		ast: AstAppend<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		match ast {
			AstAppend::IntoVariable {
				target,
				source,
				..
			} => {
				let target_text = target.name();
				let target = crate::bump::BumpFragment::internal(self.bump, target_text);

				let source = match source {
					AstAppendSource::Statement(statement) => {
						let plans = self.compile(statement, tx)?;
						AppendSourcePlan::Statement(plans)
					}
					AstAppendSource::Inline(list) => {
						let inline = compile_inline_list(list)?;
						AppendSourcePlan::Inline(inline)
					}
				};

				Ok(LogicalPlan::Append(AppendNode::IntoVariable {
					target,
					source,
				}))
			}
			AstAppend::Query {
				with,
				..
			} => {
				let with = self.compile(with.statement, tx)?;
				Ok(LogicalPlan::Append(AppendNode::Query {
					with,
				}))
			}
		}
	}
}

fn compile_inline_list(list: AstList<'_>) -> crate::Result<InlineDataNode> {
	let mut rows = Vec::new();

	for row in list.nodes {
		match row {
			Ast::Inline(row) => {
				let mut alias_fields = Vec::new();
				for field in row.keyed_values {
					let key_fragment = field.key.token.fragment.to_owned();
					let alias = IdentExpression(key_fragment.clone());
					let expr = ExpressionCompiler::compile(BumpBox::into_inner(field.value))?;

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
					message: "Expected inline data row".to_string(),
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

	Ok(InlineDataNode {
		rows,
	})
}
