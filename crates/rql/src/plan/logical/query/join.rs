// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::JoinType;

use crate::{
	ast::{Ast, AstFrom, AstInfix, AstJoin, InfixOperator, identifier::UnresolvedSourceIdentifier},
	expression::JoinConditionCompiler,
	plan::logical::{
		Compiler, JoinInnerNode, JoinLeftNode, JoinNaturalNode, LogicalPlan, LogicalPlan::SourceScan,
		SourceScanNode, resolver,
	},
};

impl Compiler {
	pub(crate) async fn compile_join<'a, T: CatalogQueryTransaction>(
		ast: AstJoin<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		match ast {
			AstJoin::InnerJoin {
				with,
				on,
				alias,
				..
			} => {
				let with_ast = with.statement.nodes.first().expect("Empty subquery in join");
				let with = match with_ast {
					Ast::From(AstFrom::Source {
						source,
						..
					}) => {
						// Handle FROM clause directly
						use crate::ast::identifier::UnresolvedSourceIdentifier;

						let mut unresolved = UnresolvedSourceIdentifier::new(
							source.namespace.clone(),
							source.name.clone(),
						);
						if let Some(a) = &alias {
							unresolved = unresolved.with_alias(a.clone());
						}

						// Build resolved source from
						// unresolved identifier
						let resolved_source =
							resolver::resolve_unresolved_source(tx, &unresolved).await?;
						vec![SourceScan(SourceScanNode {
							source: resolved_source,
							columns: None,
							index: None,
						})]
					}
					Ast::Identifier(identifier) => {
						// Create unresolved source
						// identifier
						use crate::ast::identifier::UnresolvedSourceIdentifier;

						let mut unresolved = UnresolvedSourceIdentifier::new(
							None,
							identifier.token.fragment.clone(),
						);
						if let Some(a) = &alias {
							unresolved = unresolved.with_alias(a.clone());
						}

						// Build resolved source from
						// unresolved identifier
						let resolved_source =
							resolver::resolve_unresolved_source(tx, &unresolved).await?;
						vec![SourceScan(SourceScanNode {
							source: resolved_source,
							columns: None,
							index: None,
						})]
					}
					Ast::Infix(AstInfix {
						left,
						operator,
						right,
						..
					}) => {
						assert!(matches!(operator, InfixOperator::AccessTable(_)));
						let Ast::Identifier(namespace) = &**left else {
							unreachable!()
						};
						let Ast::Identifier(table) = &**right else {
							unreachable!()
						};
						// Create fully qualified
						// SourceIdentifier
						use crate::ast::identifier::UnresolvedSourceIdentifier;

						let mut unresolved = UnresolvedSourceIdentifier::new(
							Some(namespace.token.fragment.clone()),
							table.token.fragment.clone(),
						);
						if let Some(a) = &alias {
							unresolved = unresolved.with_alias(a.clone());
						}

						// Build resolved source from
						// unresolved identifier
						let resolved_source =
							resolver::resolve_unresolved_source(tx, &unresolved).await?;
						vec![SourceScan(SourceScanNode {
							source: resolved_source,
							columns: None,
							index: None,
						})]
					}
					_ => unimplemented!(),
				};

				// Use JoinConditionCompiler for ON clause expressions
				let join_compiler = JoinConditionCompiler::new(alias.clone());
				Ok(LogicalPlan::JoinInner(JoinInnerNode {
					with,
					on: on.into_iter()
						.map(|expr| join_compiler.compile(expr))
						.collect::<crate::Result<Vec<_>>>()?,
					alias,
				}))
			}
			AstJoin::LeftJoin {
				with,
				on,
				alias,
				..
			} => {
				let with_ast = with.statement.nodes.first().expect("Empty subquery in join");
				let with = match with_ast {
					Ast::From(AstFrom::Source {
						source,
						..
					}) => {
						// Handle FROM clause directly
						use crate::ast::identifier::UnresolvedSourceIdentifier;

						let mut unresolved = UnresolvedSourceIdentifier::new(
							source.namespace.clone(),
							source.name.clone(),
						);
						if let Some(a) = &alias {
							unresolved = unresolved.with_alias(a.clone());
						}

						// Build resolved source from
						// unresolved identifier
						let resolved_source =
							resolver::resolve_unresolved_source(tx, &unresolved).await?;
						vec![SourceScan(SourceScanNode {
							source: resolved_source,
							columns: None,
							index: None,
						})]
					}
					Ast::Identifier(identifier) => {
						// Create unresolved source
						// identifier
						use crate::ast::identifier::UnresolvedSourceIdentifier;

						let mut unresolved = UnresolvedSourceIdentifier::new(
							None,
							identifier.token.fragment.clone(),
						);
						if let Some(a) = &alias {
							unresolved = unresolved.with_alias(a.clone());
						}

						// Build resolved source from
						// unresolved identifier
						let resolved_source =
							resolver::resolve_unresolved_source(tx, &unresolved).await?;
						vec![SourceScan(SourceScanNode {
							source: resolved_source,
							columns: None,
							index: None,
						})]
					}
					Ast::Infix(AstInfix {
						left,
						operator,
						right,
						..
					}) => {
						assert!(matches!(operator, InfixOperator::AccessTable(_)));
						let Ast::Identifier(namespace) = &**left else {
							unreachable!()
						};
						let Ast::Identifier(table) = &**right else {
							unreachable!()
						};
						// Create fully qualified
						// SourceIdentifier
						use crate::ast::identifier::UnresolvedSourceIdentifier;

						let mut unresolved = UnresolvedSourceIdentifier::new(
							Some(namespace.token.fragment.clone()),
							table.token.fragment.clone(),
						);
						if let Some(a) = &alias {
							unresolved = unresolved.with_alias(a.clone());
						}

						// Build resolved source from
						// unresolved identifier
						let resolved_source =
							resolver::resolve_unresolved_source(tx, &unresolved).await?;
						vec![SourceScan(SourceScanNode {
							source: resolved_source,
							columns: None,
							index: None,
						})]
					}
					_ => unimplemented!(),
				};

				// Use JoinConditionCompiler for ON clause expressions
				let join_compiler = JoinConditionCompiler::new(alias.clone());
				Ok(LogicalPlan::JoinLeft(JoinLeftNode {
					with,
					on: on.into_iter()
						.map(|expr| join_compiler.compile(expr))
						.collect::<crate::Result<Vec<_>>>()?,
					alias,
				}))
			}
			AstJoin::NaturalJoin {
				with,
				join_type,
				alias,
				..
			} => {
				// Subqueries should always have at least one node
				let with_ast = with.statement.nodes.first().expect("Empty subquery in join");
				let with = match with_ast {
					Ast::From(AstFrom::Source {
						source,
						..
					}) => {
						// Handle FROM clause directly
						use crate::ast::identifier::UnresolvedSourceIdentifier;

						let mut unresolved = UnresolvedSourceIdentifier::new(
							source.namespace.clone(),
							source.name.clone(),
						);
						if let Some(a) = &alias {
							unresolved = unresolved.with_alias(a.clone());
						}

						// Build resolved source from
						// unresolved identifier
						let resolved_source =
							resolver::resolve_unresolved_source(tx, &unresolved).await?;
						vec![SourceScan(SourceScanNode {
							source: resolved_source,
							columns: None,
							index: None,
						})]
					}
					Ast::Identifier(identifier) => {
						// Create unresolved source
						// identifier
						use crate::ast::identifier::UnresolvedSourceIdentifier;

						let mut unresolved = UnresolvedSourceIdentifier::new(
							None,
							identifier.token.fragment.clone(),
						);
						if let Some(a) = &alias {
							unresolved = unresolved.with_alias(a.clone());
						}

						// Build resolved source from
						// unresolved identifier
						let resolved_source =
							resolver::resolve_unresolved_source(tx, &unresolved).await?;
						vec![SourceScan(SourceScanNode {
							source: resolved_source,
							columns: None,
							index: None,
						})]
					}
					Ast::Infix(AstInfix {
						left,
						operator,
						right,
						..
					}) => {
						assert!(matches!(operator, InfixOperator::AccessTable(_)));
						let Ast::Identifier(namespace) = &**left else {
							unreachable!()
						};
						let Ast::Identifier(table) = &**right else {
							unreachable!()
						};

						let unresolved = UnresolvedSourceIdentifier::new(
							Some(namespace.token.fragment.clone()),
							table.token.fragment.clone(),
						);

						let resolved_source =
							resolver::resolve_unresolved_source(tx, &unresolved).await?;

						vec![SourceScan(SourceScanNode {
							source: resolved_source,
							columns: None,
							index: None,
						})]
					}
					_ => unimplemented!(),
				};

				Ok(LogicalPlan::JoinNatural(JoinNaturalNode {
					with,
					join_type: join_type.unwrap_or(JoinType::Inner),
					alias,
				}))
			}
		}
	}
}
