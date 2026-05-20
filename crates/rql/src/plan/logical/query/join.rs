// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::common::JoinType;
use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	ast::{
		ast::{Ast, AstFrom, AstInfix, AstJoin, AstSubQuery, AstUsingClause, InfixOperator, JoinConnector},
		identifier::UnresolvedShapeIdentifier,
	},
	bump::{BumpBox, BumpFragment, BumpVec},
	expression::{AndExpression, EqExpression, Expression, OrExpression, join::JoinConditionCompiler},
	plan::logical::{
		Compiler, JoinInnerNode, JoinLeftNode, JoinNaturalNode, LogicalPlan,
		LogicalPlan::PrimitiveScan,
		PipelineNode, RemoteScanNode, ShapeScanNode,
		resolver::{self, ResolvedSource},
	},
};

fn build_join_expressions(using: AstUsingClause<'_>, alias: &BumpFragment<'_>) -> Result<Vec<Expression>> {
	let compiler = JoinConditionCompiler::new(Some(alias.to_owned()));
	let fragment = using.token.fragment.to_owned();

	let use_or = using.pairs.iter().any(|p| matches!(p.connector, Some(JoinConnector::Or)));

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

	if eq_exprs.len() == 1 {
		return Ok(eq_exprs);
	}

	let combined = if use_or {
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
				ttl,
				snapshot,
				rql,
				..
			} => {
				let with = self.compile_join_subquery(with, &alias, tx)?;
				let on = build_join_expressions(using_clause, &alias)?;
				let ttl = match ttl {
					Some(ast_ttl) => Some(Self::compile_join_ttl(ast_ttl)?),
					None => None,
				};

				Ok(LogicalPlan::JoinInner(JoinInnerNode {
					with,
					on,
					alias: Some(alias),
					ttl,
					snapshot,
					rql: rql.to_string(),
				}))
			}
			AstJoin::LeftJoin {
				with,
				using_clause,
				alias,
				ttl,
				snapshot,
				rql,
				..
			} => {
				let with = self.compile_join_subquery(with, &alias, tx)?;
				let on = build_join_expressions(using_clause, &alias)?;
				let ttl = match ttl {
					Some(ast_ttl) => Some(Self::compile_join_ttl(ast_ttl)?),
					None => None,
				};

				Ok(LogicalPlan::JoinLeft(JoinLeftNode {
					with,
					on,
					alias: Some(alias),
					ttl,
					snapshot,
					rql: rql.to_string(),
				}))
			}
			AstJoin::NaturalJoin {
				with,
				join_type,
				alias,
				ttl,
				snapshot,
				rql,
				..
			} => {
				let with = self.compile_natural_join_subquery(with, &alias, tx)?;
				let ttl = match ttl {
					Some(ast_ttl) => Some(Self::compile_join_ttl(ast_ttl)?),
					None => None,
				};

				Ok(LogicalPlan::JoinNatural(JoinNaturalNode {
					with,
					join_type: join_type.unwrap_or(JoinType::Inner),
					alias: Some(alias),
					ttl,
					snapshot,
					rql: rql.to_string(),
				}))
			}
		}
	}

	fn compile_join_subquery(
		&self,
		with: AstSubQuery<'bump>,
		alias: &BumpFragment<'_>,
		tx: &mut Transaction<'_>,
	) -> Result<BumpVec<'bump, LogicalPlan<'bump>>> {
		self.compile_join_subquery_nodes(with, alias, tx)
	}

	fn compile_natural_join_subquery(
		&self,
		with: AstSubQuery<'bump>,
		alias: &BumpFragment<'_>,
		tx: &mut Transaction<'_>,
	) -> Result<BumpVec<'bump, LogicalPlan<'bump>>> {
		self.compile_join_subquery_nodes(with, alias, tx)
	}

	fn compile_join_subquery_nodes(
		&self,
		with: AstSubQuery<'bump>,
		alias: &BumpFragment<'_>,
		tx: &mut Transaction<'_>,
	) -> Result<BumpVec<'bump, LogicalPlan<'bump>>> {
		let mut nodes = with.statement.nodes.into_iter();
		let first = nodes.next().expect("Empty subquery in join");

		let source_plan = match first {
			Ast::From(AstFrom::Source {
				source,
				..
			}) => {
				let mut unresolved =
					UnresolvedShapeIdentifier::new(source.namespace.clone(), source.name);
				unresolved = unresolved.with_alias(*alias);
				resolve_join_plan(&self.catalog, tx, &unresolved)?
			}
			Ast::Identifier(identifier) => {
				let mut unresolved = UnresolvedShapeIdentifier::new(vec![], identifier.token.fragment);
				unresolved = unresolved.with_alias(*alias);
				resolve_join_plan(&self.catalog, tx, &unresolved)?
			}
			Ast::Infix(AstInfix {
				left,
				operator,
				right,
				..
			}) => {
				assert!(matches!(operator, InfixOperator::AccessTable(_)));
				let Ast::Identifier(namespace) = &*left else {
					unreachable!()
				};
				let Ast::Identifier(table) = &*right else {
					unreachable!()
				};

				let mut unresolved = UnresolvedShapeIdentifier::new(
					vec![namespace.token.fragment],
					table.token.fragment,
				);
				unresolved = unresolved.with_alias(*alias);
				resolve_join_plan(&self.catalog, tx, &unresolved)?
			}
			_ => unimplemented!(),
		};

		let remaining: Vec<LogicalPlan<'bump>> =
			nodes.map(|node| self.compile_single(node, tx)).collect::<Result<_>>()?;

		let mut result = BumpVec::with_capacity_in(1, self.bump);
		if remaining.is_empty() {
			result.push(source_plan);
		} else {
			let mut steps = BumpVec::with_capacity_in(1 + remaining.len(), self.bump);
			steps.push(source_plan);
			for plan in remaining {
				steps.push(plan);
			}
			result.push(LogicalPlan::Pipeline(PipelineNode {
				steps,
			}));
		}
		Ok(result)
	}
}

fn resolve_join_plan<'bump>(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	unresolved: &UnresolvedShapeIdentifier,
) -> Result<LogicalPlan<'bump>> {
	let resolved = resolver::resolve_unresolved_source(catalog, tx, unresolved)?;
	match resolved {
		ResolvedSource::Shape(p) => Ok(PrimitiveScan(ShapeScanNode {
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
