// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	evaluate::expression::{AliasExpression, IdentExpression},
	identifier::{IndexIdentifier, SourceIdentifier},
};
use reifydb_type::{Fragment, OwnedFragment, diagnostic::Diagnostic, err};

use crate::{
	ast::{Ast, AstFrom},
	expression::ExpressionCompiler,
	plan::logical::{
		Compiler, InlineDataNode, LogicalPlan, SourceScanNode,
	},
};

impl Compiler {
	pub(crate) fn compile_from<'a>(
		ast: AstFrom<'a>,
	) -> crate::Result<LogicalPlan<'a>> {
		match ast {
			AstFrom::Source {
				source,
				index_name,
				..
			} => {
				// Convert MaybeQualifiedSourceIdentifier to
				// SourceIdentifier
				let schema = source
					.schema
					.clone()
					.unwrap_or_else(|| {
						Fragment::Owned(OwnedFragment::Internal { text: String::from("default") })
					});
				let qualified_source = SourceIdentifier::new(
					schema.clone(),
					source.name.clone(),
					source.kind,
				);
				let qualified_source = if let Some(alias) =
					&source.alias
				{
					qualified_source
						.with_alias(alias.clone())
				} else {
					qualified_source
				};

				// Convert index_name to IndexIdentifier if
				// present
				let index = index_name.map(|idx| {
					IndexIdentifier::new(
						schema,
						source.name.clone(),
						idx.fragment(),
					)
				});

				Ok(LogicalPlan::SourceScan(SourceScanNode {
					source: qualified_source,
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
							let mut alias_fields =
								Vec::new();
							for field in
								row.keyed_values
							{
								let key_fragment = field.key.fragment();
								let alias = IdentExpression(key_fragment.clone());
								let expr =
                                    ExpressionCompiler::compile(field.value.as_ref().clone())?;

								let alias_expr = AliasExpression {
                                    alias,
                                    expression: Box::new(expr),
                                    fragment: key_fragment};
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
                                cause: None});
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

#[cfg(test)]
mod tests {
	use reifydb_core::interface::evaluate::expression::{
		ConstantExpression, Expression,
	};

	use super::*;
	use crate::ast::{parse::parse, tokenize::tokenize};

	#[test]
	fn test_compile_static_single_row() {
		let tokens = tokenize("from [{id: 1, name: 'Alice'}]").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let from_ast = result[0].first_unchecked().as_from().clone();
		let logical_plan = Compiler::compile_from(from_ast).unwrap();

		match logical_plan {
			LogicalPlan::InlineData(node) => {
				assert_eq!(node.rows.len(), 1); // One row
				assert_eq!(node.rows[0].len(), 2); // Two KeyedExpressions: id and name
				assert_eq!(
					node.rows[0][0].alias.0.fragment(),
					"id"
				);
				assert_eq!(
					node.rows[0][1].alias.0.fragment(),
					"name"
				);
			}
			_ => panic!("Expected InlineData node"),
		}
	}

	#[test]
	fn test_compile_static_multiple_rows_different_columns() {
		let tokens =
            tokenize("from [{id: 1, name: 'Alice'}, {id: 2, email: 'bob@test.com'}, {name: 'Charlie'}]")
                .unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let from_ast = result[0].first_unchecked().as_from().clone();
		let logical_plan = Compiler::compile_from(from_ast).unwrap();

		match logical_plan {
			LogicalPlan::InlineData(node) => {
				// Should have 3 rows
				assert_eq!(node.rows.len(), 3);

				// First row: id: 1, name: 'Alice'
				assert_eq!(node.rows[0].len(), 2);
				assert_eq!(
					node.rows[0][0].alias.0.fragment(),
					"id"
				);
				assert_eq!(
					node.rows[0][1].alias.0.fragment(),
					"name"
				);

				// Second row: id: 2, email: 'bob@test.com'
				assert_eq!(node.rows[1].len(), 2);
				assert_eq!(
					node.rows[1][0].alias.0.fragment(),
					"id"
				);
				assert_eq!(
					node.rows[1][1].alias.0.fragment(),
					"email"
				);

				// Third row: name: 'Charlie'
				assert_eq!(node.rows[2].len(), 1);
				assert_eq!(
					node.rows[2][0].alias.0.fragment(),
					"name"
				);

				// Check some expression values
				match &*node.rows[0][0].expression {
					Expression::Constant(
						ConstantExpression::Number {
							fragment,
						},
					) => {
						assert_eq!(
							fragment.fragment(),
							"1"
						);
					}
					_ => panic!(
						"Expected Number for id in first row"
					),
				}

				match &*node.rows[0][1].expression {
					Expression::Constant(
						ConstantExpression::Text {
							fragment,
						},
					) => {
						assert_eq!(
							fragment.fragment(),
							"Alice"
						);
					}
					_ => panic!(
						"Expected Text for name in first row"
					),
				}
			}
			_ => panic!("Expected InlineData node"),
		}
	}

	#[test]
	fn test_compile_static_empty_list() {
		let tokens = tokenize("from []").unwrap();
		let result = parse(tokens).unwrap();
		assert_eq!(result.len(), 1);

		let from_ast = result[0].first_unchecked().as_from().clone();
		let logical_plan = Compiler::compile_from(from_ast).unwrap();

		match logical_plan {
			LogicalPlan::InlineData(node) => {
				assert_eq!(node.rows.len(), 0);
			}
			_ => panic!("Expected InlineData node"),
		}
	}
}
