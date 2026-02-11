// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::common::JoinType;
use reifydb_transaction::transaction::AsTransaction;

use crate::{
	ast::{
		ast::{Ast, AstFrom, AstInfix, AstJoin, AstUsingClause, InfixOperator, JoinConnector},
		identifier::UnresolvedPrimitiveIdentifier,
	},
	bump::{BumpBox, BumpFragment, BumpVec},
	expression::{AndExpression, EqExpression, Expression, OrExpression, join::JoinConditionCompiler},
	plan::logical::{
		Compiler, JoinInnerNode, JoinLeftNode, JoinNaturalNode, LogicalPlan, LogicalPlan::PrimitiveScan,
		PrimitiveScanNode, resolver,
	},
};

/// Build expression tree from using clause pairs.
/// Each pair (expr1, expr2) becomes an equality condition expr1 == expr2.
/// Pairs are combined using AND or OR based on their connectors.
fn build_join_expressions(using: AstUsingClause<'_>, alias: &BumpFragment<'_>) -> crate::Result<Vec<Expression>> {
	let compiler = JoinConditionCompiler::new(Some(alias.to_owned()));
	let fragment = using.token.fragment.to_owned();

	// Check if any connector is OR (determines the overall combination strategy)
	let use_or = using.pairs.iter().any(|p| matches!(p.connector, Some(JoinConnector::Or)));

	// Build equality expressions for each pair
	let mut eq_exprs: Vec<Expression> = Vec::new();
	for pair in using.pairs {
		let left_expr = compiler.compile(BumpBox::into_inner(pair.first))?;
		let right_expr = compiler.compile(BumpBox::into_inner(pair.second))?;
		eq_exprs.push(Expression::Equal(EqExpression {
			left: Box::new(left_expr),
			right: Box::new(right_expr),
			fragment: fragment.clone(),
		}));
	}

	// If only one expression, return it directly
	if eq_exprs.len() == 1 {
		return Ok(eq_exprs);
	}

	// Build the combined expression
	let combined = if use_or {
		// OR all expressions together
		eq_exprs.into_iter()
			.reduce(|acc, expr| {
				Expression::Or(OrExpression {
					left: Box::new(acc),
					right: Box::new(expr),
					fragment: fragment.clone(),
				})
			})
			.unwrap()
	} else {
		// AND all expressions together (default)
		eq_exprs.into_iter()
			.reduce(|acc, expr| {
				Expression::And(AndExpression {
					left: Box::new(acc),
					right: Box::new(expr),
					fragment: fragment.clone(),
				})
			})
			.unwrap()
	};

	Ok(vec![combined])
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_join<T: AsTransaction>(
		&self,
		ast: AstJoin<'bump>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'bump>> {
		match ast {
			AstJoin::InnerJoin {
				with,
				using_clause,
				alias,
				..
			} => {
				let with = self.compile_join_subquery(&with, &alias, tx)?;

				// Build equality expressions from using clause
				let on = build_join_expressions(using_clause, &alias)?;

				Ok(LogicalPlan::JoinInner(JoinInnerNode {
					with,
					on,
					alias: Some(alias),
				}))
			}
			AstJoin::LeftJoin {
				with,
				using_clause,
				alias,
				..
			} => {
				let with = self.compile_join_subquery(&with, &alias, tx)?;

				// Build equality expressions from using clause
				let on = build_join_expressions(using_clause, &alias)?;

				Ok(LogicalPlan::JoinLeft(JoinLeftNode {
					with,
					on,
					alias: Some(alias),
				}))
			}
			AstJoin::NaturalJoin {
				with,
				join_type,
				alias,
				..
			} => {
				let with = self.compile_natural_join_subquery(&with, &alias, tx)?;

				Ok(LogicalPlan::JoinNatural(JoinNaturalNode {
					with,
					join_type: join_type.unwrap_or(JoinType::Inner),
					alias: Some(alias),
				}))
			}
		}
	}

	fn compile_join_subquery<T: AsTransaction>(
		&self,
		with: &crate::ast::ast::AstSubQuery,
		alias: &BumpFragment<'_>,
		tx: &mut T,
	) -> crate::Result<BumpVec<'bump, LogicalPlan<'bump>>> {
		let with_ast = with.statement.nodes.first().expect("Empty subquery in join");
		match with_ast {
			Ast::From(AstFrom::Source {
				source,
				..
			}) => {
				let mut unresolved =
					UnresolvedPrimitiveIdentifier::new(source.namespace.clone(), source.name);
				unresolved = unresolved.with_alias(*alias);

				let resolved_source =
					resolver::resolve_unresolved_source(&self.catalog, tx, &unresolved)?;
				let mut result = BumpVec::with_capacity_in(1, self.bump);
				result.push(PrimitiveScan(PrimitiveScanNode {
					source: resolved_source,
					columns: None,
					index: None,
				}));
				Ok(result)
			}
			Ast::Identifier(identifier) => {
				let mut unresolved =
					UnresolvedPrimitiveIdentifier::new(vec![], identifier.token.fragment);
				unresolved = unresolved.with_alias(*alias);

				let resolved_source =
					resolver::resolve_unresolved_source(&self.catalog, tx, &unresolved)?;
				let mut result = BumpVec::with_capacity_in(1, self.bump);
				result.push(PrimitiveScan(PrimitiveScanNode {
					source: resolved_source,
					columns: None,
					index: None,
				}));
				Ok(result)
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

				let mut unresolved = UnresolvedPrimitiveIdentifier::new(
					vec![namespace.token.fragment],
					table.token.fragment,
				);
				unresolved = unresolved.with_alias(*alias);

				let resolved_source =
					resolver::resolve_unresolved_source(&self.catalog, tx, &unresolved)?;
				let mut result = BumpVec::with_capacity_in(1, self.bump);
				result.push(PrimitiveScan(PrimitiveScanNode {
					source: resolved_source,
					columns: None,
					index: None,
				}));
				Ok(result)
			}
			_ => unimplemented!(),
		}
	}

	fn compile_natural_join_subquery<T: AsTransaction>(
		&self,
		with: &crate::ast::ast::AstSubQuery,
		alias: &BumpFragment<'_>,
		tx: &mut T,
	) -> crate::Result<BumpVec<'bump, LogicalPlan<'bump>>> {
		let with_ast = with.statement.nodes.first().expect("Empty subquery in join");
		match with_ast {
			Ast::From(AstFrom::Source {
				source,
				..
			}) => {
				let mut unresolved =
					UnresolvedPrimitiveIdentifier::new(source.namespace.clone(), source.name);
				unresolved = unresolved.with_alias(*alias);

				let resolved_source =
					resolver::resolve_unresolved_source(&self.catalog, tx, &unresolved)?;
				let mut result = BumpVec::with_capacity_in(1, self.bump);
				result.push(PrimitiveScan(PrimitiveScanNode {
					source: resolved_source,
					columns: None,
					index: None,
				}));
				Ok(result)
			}
			Ast::Identifier(identifier) => {
				let mut unresolved =
					UnresolvedPrimitiveIdentifier::new(vec![], identifier.token.fragment);
				unresolved = unresolved.with_alias(*alias);

				let resolved_source =
					resolver::resolve_unresolved_source(&self.catalog, tx, &unresolved)?;
				let mut result = BumpVec::with_capacity_in(1, self.bump);
				result.push(PrimitiveScan(PrimitiveScanNode {
					source: resolved_source,
					columns: None,
					index: None,
				}));
				Ok(result)
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

				let mut unresolved = UnresolvedPrimitiveIdentifier::new(
					vec![namespace.token.fragment],
					table.token.fragment,
				);
				unresolved = unresolved.with_alias(*alias);

				let resolved_source =
					resolver::resolve_unresolved_source(&self.catalog, tx, &unresolved)?;
				let mut result = BumpVec::with_capacity_in(1, self.bump);
				result.push(PrimitiveScan(PrimitiveScanNode {
					source: resolved_source,
					columns: None,
					index: None,
				}));
				Ok(result)
			}
			_ => unimplemented!(),
		}
	}
}
