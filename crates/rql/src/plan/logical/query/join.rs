// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::common::JoinType;
use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	ast::{
		ast::{Ast, AstFrom, AstInfix, AstJoin, AstSubQuery, AstUsingClause, InfixOperator, JoinConnector},
		identifier::UnresolvedPrimitiveIdentifier,
	},
	bump::{BumpBox, BumpFragment, BumpVec},
	expression::{AndExpression, EqExpression, Expression, OrExpression, join::JoinConditionCompiler},
	plan::logical::{
		Compiler, JoinInnerNode, JoinLeftNode, JoinNaturalNode, LogicalPlan,
		LogicalPlan::PrimitiveScan,
		PrimitiveScanNode, RemoteScanNode,
		resolver::{self, ResolvedSource},
	},
};

/// Build expression tree from using clause pairs.
/// Each pair (expr1, expr2) becomes an equality condition expr1 == expr2.
/// Pairs are combined using AND or OR based on their connectors.
fn build_join_expressions(using: AstUsingClause<'_>, alias: &BumpFragment<'_>) -> Result<Vec<Expression>> {
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
	pub(crate) fn compile_join(&self, ast: AstJoin<'bump>, tx: &mut Transaction<'_>) -> Result<LogicalPlan<'bump>> {
		match ast {
			AstJoin::InnerJoin {
				with,
				using_clause,
				alias,
				rql,
				..
			} => {
				let with = self.compile_join_subquery(&with, &alias, tx)?;

				// Build equality expressions from using clause
				let on = build_join_expressions(using_clause, &alias)?;

				Ok(LogicalPlan::JoinInner(JoinInnerNode {
					with,
					on,
					alias: Some(alias),
					rql: rql.to_string(),
				}))
			}
			AstJoin::LeftJoin {
				with,
				using_clause,
				alias,
				rql,
				..
			} => {
				let with = self.compile_join_subquery(&with, &alias, tx)?;

				// Build equality expressions from using clause
				let on = build_join_expressions(using_clause, &alias)?;

				Ok(LogicalPlan::JoinLeft(JoinLeftNode {
					with,
					on,
					alias: Some(alias),
					rql: rql.to_string(),
				}))
			}
			AstJoin::NaturalJoin {
				with,
				join_type,
				alias,
				rql,
				..
			} => {
				let with = self.compile_natural_join_subquery(&with, &alias, tx)?;

				Ok(LogicalPlan::JoinNatural(JoinNaturalNode {
					with,
					join_type: join_type.unwrap_or(JoinType::Inner),
					alias: Some(alias),
					rql: rql.to_string(),
				}))
			}
		}
	}

	fn compile_join_subquery(
		&self,
		with: &AstSubQuery,
		alias: &BumpFragment<'_>,
		tx: &mut Transaction<'_>,
	) -> Result<BumpVec<'bump, LogicalPlan<'bump>>> {
		let with_ast = with.statement.nodes.first().expect("Empty subquery in join");
		match with_ast {
			Ast::From(AstFrom::Source {
				source,
				..
			}) => {
				let mut unresolved =
					UnresolvedPrimitiveIdentifier::new(source.namespace.clone(), source.name);
				unresolved = unresolved.with_alias(*alias);

				let plan = resolve_join_plan(&self.catalog, tx, &unresolved)?;
				let mut result = BumpVec::with_capacity_in(1, self.bump);
				result.push(plan);
				Ok(result)
			}
			Ast::Identifier(identifier) => {
				let mut unresolved =
					UnresolvedPrimitiveIdentifier::new(vec![], identifier.token.fragment);
				unresolved = unresolved.with_alias(*alias);

				let plan = resolve_join_plan(&self.catalog, tx, &unresolved)?;
				let mut result = BumpVec::with_capacity_in(1, self.bump);
				result.push(plan);
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

				let plan = resolve_join_plan(&self.catalog, tx, &unresolved)?;
				let mut result = BumpVec::with_capacity_in(1, self.bump);
				result.push(plan);
				Ok(result)
			}
			_ => unimplemented!(),
		}
	}

	fn compile_natural_join_subquery(
		&self,
		with: &AstSubQuery,
		alias: &BumpFragment<'_>,
		tx: &mut Transaction<'_>,
	) -> Result<BumpVec<'bump, LogicalPlan<'bump>>> {
		let with_ast = with.statement.nodes.first().expect("Empty subquery in join");
		match with_ast {
			Ast::From(AstFrom::Source {
				source,
				..
			}) => {
				let mut unresolved =
					UnresolvedPrimitiveIdentifier::new(source.namespace.clone(), source.name);
				unresolved = unresolved.with_alias(*alias);

				let plan = resolve_join_plan(&self.catalog, tx, &unresolved)?;
				let mut result = BumpVec::with_capacity_in(1, self.bump);
				result.push(plan);
				Ok(result)
			}
			Ast::Identifier(identifier) => {
				let mut unresolved =
					UnresolvedPrimitiveIdentifier::new(vec![], identifier.token.fragment);
				unresolved = unresolved.with_alias(*alias);

				let plan = resolve_join_plan(&self.catalog, tx, &unresolved)?;
				let mut result = BumpVec::with_capacity_in(1, self.bump);
				result.push(plan);
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

				let plan = resolve_join_plan(&self.catalog, tx, &unresolved)?;
				let mut result = BumpVec::with_capacity_in(1, self.bump);
				result.push(plan);
				Ok(result)
			}
			_ => unimplemented!(),
		}
	}
}

fn resolve_join_plan<'bump>(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	unresolved: &UnresolvedPrimitiveIdentifier,
) -> Result<LogicalPlan<'bump>> {
	let resolved = resolver::resolve_unresolved_source(catalog, tx, unresolved)?;
	match resolved {
		ResolvedSource::Primitive(p) => Ok(PrimitiveScan(PrimitiveScanNode {
			source: p,
			columns: None,
			index: None,
		})),
		ResolvedSource::Remote {
			address,
			token,
			local_namespace,
			remote_name,
		} => Ok(LogicalPlan::RemoteScan(RemoteScanNode {
			address,
			token,
			local_namespace,
			remote_name,
		})),
	}
}
