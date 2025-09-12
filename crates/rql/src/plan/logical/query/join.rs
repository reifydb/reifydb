// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::JoinType;

use crate::{
	ast::{Ast, AstInfix, AstJoin, InfixOperator},
	expression::ExpressionCompiler,
	plan::logical::{
		Compiler, JoinInnerNode, JoinLeftNode, JoinNaturalNode,
		LogicalPlan, LogicalPlan::SourceScan, SourceScanNode,
		resolver::IdentifierResolver,
	},
};

impl Compiler {
	pub(crate) fn compile_join<'a, 't, T: CatalogQueryTransaction>(
		ast: AstJoin<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		match ast {
			AstJoin::InnerJoin {
				with,
				on,
				alias,
				..
			} => {
				let with = match *with {
					Ast::Identifier(identifier) => {
						// Create fully qualified
						// SourceIdentifier
						use reifydb_core::interface::identifier::{SourceIdentifier, SourceKind};
						use reifydb_type::{Fragment, OwnedFragment};

						let namespace = Fragment::Owned(OwnedFragment::Internal { text: String::from("default") });
						let mut source = SourceIdentifier::new(namespace, identifier.token.fragment.clone(), SourceKind::Unknown);
						if let Some(a) = alias {
							source = source
								.with_alias(a);
						}

						// Build resolved source
						let resolved_source = resolver
							.build_resolved_source(
								source,
							)?;
						vec![SourceScan(
							SourceScanNode {
								source: resolved_source,
								columns: None,
								index: None,
							},
						)]
					}
					Ast::Infix(AstInfix {
						left,
						operator,
						right,
						..
					}) => {
						assert!(matches!(operator, InfixOperator::AccessTable(_)));
						let Ast::Identifier(namespace) =
							*left
						else {
							unreachable!()
						};
						let Ast::Identifier(table) =
							*right
						else {
							unreachable!()
						};
						// Create fully qualified
						// SourceIdentifier
						use reifydb_core::interface::identifier::{SourceIdentifier, SourceKind};

						let mut source = SourceIdentifier::new(namespace.token.fragment, table.token.fragment, SourceKind::Unknown);
						if let Some(a) = alias {
							source = source
								.with_alias(a);
						}

						// Build resolved source
						let resolved_source = resolver
							.build_resolved_source(
								source,
							)?;
						vec![SourceScan(
							SourceScanNode {
								source: resolved_source,
								columns: None,
								index: None,
							},
						)]
					}
					_ => unimplemented!(),
				};
				Ok(LogicalPlan::JoinInner(JoinInnerNode {
                    with,
                    on: on
                        .into_iter()
                        .map(ExpressionCompiler::compile)
                        .collect::<crate::Result<Vec<_>>>()?,
				}))
			}
			AstJoin::LeftJoin {
				with,
				on,
				alias,
				..
			} => {
				let with = match *with {
					Ast::Identifier(identifier) => {
						// Create fully qualified
						// SourceIdentifier
						use reifydb_core::interface::identifier::{SourceIdentifier, SourceKind};
						use reifydb_type::{Fragment, OwnedFragment};

						let namespace = Fragment::Owned(OwnedFragment::Internal { text: String::from("default") });
						let mut source = SourceIdentifier::new(namespace, identifier.token.fragment.clone(), SourceKind::Unknown);
						if let Some(a) = alias {
							source = source
								.with_alias(a);
						}

						// Build resolved source
						let resolved_source = resolver
							.build_resolved_source(
								source,
							)?;
						vec![SourceScan(
							SourceScanNode {
								source: resolved_source,
								columns: None,
								index: None,
							},
						)]
					}
					Ast::Infix(AstInfix {
						left,
						operator,
						right,
						..
					}) => {
						assert!(matches!(operator, InfixOperator::AccessTable(_)));
						let Ast::Identifier(namespace) =
							*left
						else {
							unreachable!()
						};
						let Ast::Identifier(table) =
							*right
						else {
							unreachable!()
						};
						// Create fully qualified
						// SourceIdentifier
						use reifydb_core::interface::identifier::{SourceIdentifier, SourceKind};

						let mut source = SourceIdentifier::new(namespace.token.fragment, table.token.fragment, SourceKind::Unknown);
						if let Some(a) = alias {
							source = source
								.with_alias(a);
						}

						// Build resolved source
						let resolved_source = resolver
							.build_resolved_source(
								source,
							)?;
						vec![SourceScan(
							SourceScanNode {
								source: resolved_source,
								columns: None,
								index: None,
							},
						)]
					}
					_ => unimplemented!(),
				};
				Ok(LogicalPlan::JoinLeft(JoinLeftNode {
                    with,
                    on: on
                        .into_iter()
                        .map(ExpressionCompiler::compile)
                        .collect::<crate::Result<Vec<_>>>()?,
				}))
			}
			AstJoin::NaturalJoin {
				with,
				join_type,
				alias,
				..
			} => {
				let with = match *with {
					Ast::Identifier(identifier) => {
						// Create fully qualified
						// SourceIdentifier
						use reifydb_core::interface::identifier::{SourceIdentifier, SourceKind};
						use reifydb_type::{Fragment, OwnedFragment};

						let namespace = Fragment::Owned(OwnedFragment::Internal { text: String::from("default") });
						let mut source = SourceIdentifier::new(namespace, identifier.token.fragment.clone(), SourceKind::Unknown);
						if let Some(a) = alias {
							source = source
								.with_alias(a);
						}

						// Build resolved source
						let resolved_source = resolver
							.build_resolved_source(
								source,
							)?;
						vec![SourceScan(
							SourceScanNode {
								source: resolved_source,
								columns: None,
								index: None,
							},
						)]
					}
					Ast::Infix(AstInfix {
						left,
						operator,
						right,
						..
					}) => {
						assert!(matches!(operator, InfixOperator::AccessTable(_)));
						let Ast::Identifier(namespace) =
							*left
						else {
							unreachable!()
						};
						let Ast::Identifier(table) =
							*right
						else {
							unreachable!()
						};
						// Create fully qualified
						// SourceIdentifier
						use reifydb_core::interface::identifier::{SourceIdentifier, SourceKind};

						let mut source = SourceIdentifier::new(namespace.token.fragment, table.token.fragment, SourceKind::Unknown);
						if let Some(a) = alias {
							source = source
								.with_alias(a);
						}

						// Build resolved source
						let resolved_source = resolver
							.build_resolved_source(
								source,
							)?;
						vec![SourceScan(
							SourceScanNode {
								source: resolved_source,
								columns: None,
								index: None,
							},
						)]
					}
					_ => unimplemented!(),
				};

				Ok(LogicalPlan::JoinNatural(JoinNaturalNode {
					with,
					join_type: join_type
						.unwrap_or(JoinType::Inner),
				}))
			}
		}
	}
}
