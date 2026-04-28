// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	ast::{
		ast::{Ast, AstDelete, AstFrom},
		identifier::{
			MaybeQualifiedRingBufferIdentifier, MaybeQualifiedSeriesIdentifier,
			MaybeQualifiedTableIdentifier, UnresolvedShapeIdentifier,
		},
	},
	bump::{BumpBox, BumpFragment, BumpVec},
	expression::{Expression, ExpressionCompiler},
	plan::logical::{
		Compiler, DeleteRingBufferNode, DeleteSeriesNode, DeleteTableNode, FilterNode, LogicalPlan,
		PipelineNode, mutate::compile_returning_clause,
	},
	token::token::Token,
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_delete(
		&self,
		ast: AstDelete<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let AstDelete {
			token,
			target,
			filter,
			take,
			returning,
		} = ast;
		let returning = compile_returning_clause(returning)?;
		let from_plan = self.compile_delete_from(token, target.clone(), tx)?;
		let filter_plan = compile_delete_filter(filter)?;
		let take_plan = self.compile_optional_take(take)?;
		let pipeline = self.assemble_delete_pipeline(from_plan, filter_plan, take_plan);
		self.wrap_delete_target(target, pipeline, returning, tx)
	}

	#[inline]
	fn compile_delete_from(
		&self,
		token: Token<'bump>,
		target: UnresolvedShapeIdentifier<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let from_ast = AstFrom::Source {
			token,
			source: target,
			index_name: None,
		};
		self.compile_from(from_ast, tx)
	}

	fn assemble_delete_pipeline(
		&self,
		from_plan: LogicalPlan<'bump>,
		filter_plan: LogicalPlan<'bump>,
		take_plan: Option<LogicalPlan<'bump>>,
	) -> LogicalPlan<'bump> {
		let capacity = if take_plan.is_some() {
			3
		} else {
			2
		};
		let mut steps = BumpVec::with_capacity_in(capacity, self.bump);
		steps.push(from_plan);
		steps.push(filter_plan);
		if let Some(take) = take_plan {
			steps.push(take);
		}
		LogicalPlan::Pipeline(PipelineNode {
			steps,
		})
	}

	fn wrap_delete_target(
		&self,
		target: UnresolvedShapeIdentifier<'bump>,
		pipeline: LogicalPlan<'bump>,
		returning: Option<Vec<Expression>>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let target_name = target.name.text();
		let name = target.name;
		let namespace = target.namespace;
		let ns_segments: Vec<&str> = namespace.iter().map(|n| n.text()).collect();

		let Some(ns) = self.catalog.find_namespace_by_segments(tx, &ns_segments)? else {
			return Ok(self.delete_table_node(name, namespace, pipeline, returning));
		};
		let namespace_id = ns.id();

		if self.catalog.find_ringbuffer_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut t = MaybeQualifiedRingBufferIdentifier::new(name);
			if !namespace.is_empty() {
				t = t.with_namespace(namespace);
			}
			return Ok(LogicalPlan::DeleteRingBuffer(DeleteRingBufferNode {
				target: t,
				input: Some(BumpBox::new_in(pipeline, self.bump)),
				returning,
			}));
		}
		if self.catalog.find_series_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut t = MaybeQualifiedSeriesIdentifier::new(name);
			if !namespace.is_empty() {
				t = t.with_namespace(namespace);
			}
			return Ok(LogicalPlan::DeleteSeries(DeleteSeriesNode {
				target: t,
				input: Some(BumpBox::new_in(pipeline, self.bump)),
				returning,
			}));
		}
		Ok(self.delete_table_node(name, namespace, pipeline, returning))
	}

	#[inline]
	fn delete_table_node(
		&self,
		name: BumpFragment<'bump>,
		namespace: Vec<BumpFragment<'bump>>,
		pipeline: LogicalPlan<'bump>,
		returning: Option<Vec<Expression>>,
	) -> LogicalPlan<'bump> {
		let mut target = MaybeQualifiedTableIdentifier::new(name);
		if !namespace.is_empty() {
			target = target.with_namespace(namespace);
		}
		LogicalPlan::DeleteTable(DeleteTableNode {
			target: Some(target),
			input: Some(BumpBox::new_in(pipeline, self.bump)),
			returning,
		})
	}
}

#[inline]
fn compile_delete_filter<'bump>(filter: BumpBox<'bump, Ast<'bump>>) -> Result<LogicalPlan<'bump>> {
	let filter_ast = match BumpBox::into_inner(filter) {
		Ast::Filter(f) => f,
		_ => unreachable!("filter should always be Ast::Filter"),
	};
	Ok(LogicalPlan::Filter(FilterNode {
		condition: ExpressionCompiler::compile(BumpBox::into_inner(filter_ast.node))?,
		rql: filter_ast.rql.to_string(),
	}))
}
