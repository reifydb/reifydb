// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	ast::{
		ast::{Ast, AstFrom, AstPatch, AstUpdate},
		identifier::{
			MaybeQualifiedRingBufferIdentifier, MaybeQualifiedSeriesIdentifier,
			MaybeQualifiedTableIdentifier, UnresolvedShapeIdentifier,
		},
	},
	bump::{BumpBox, BumpFragment, BumpVec},
	expression::{Expression, ExpressionCompiler},
	plan::logical::{
		Compiler, FilterNode, LogicalPlan, PipelineNode, UpdateRingBufferNode, UpdateSeriesNode,
		UpdateTableNode, mutate::compile_returning_clause,
	},
	token::token::Token,
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_update(
		&self,
		ast: AstUpdate<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let AstUpdate {
			token,
			target,
			assignments,
			filter,
			take,
			returning,
		} = ast;
		let returning = compile_returning_clause(returning)?;
		let from_plan = self.compile_update_from(token, target.clone(), tx)?;
		let filter_plan = compile_update_filter(filter)?;
		let patch_plan = self.compile_update_patch(token, assignments)?;
		let take_plan = self.compile_optional_take(take)?;
		let pipeline = self.assemble_update_pipeline(from_plan, filter_plan, take_plan, patch_plan);
		self.wrap_update_target(target, pipeline, returning, tx)
	}

	#[inline]
	fn compile_update_from(
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

	#[inline]
	fn compile_update_patch(
		&self,
		token: Token<'bump>,
		assignments: Vec<Ast<'bump>>,
	) -> Result<LogicalPlan<'bump>> {
		let patch_ast = AstPatch {
			token,
			assignments,
			rql: "",
		};
		self.compile_patch(patch_ast)
	}

	#[inline]
	pub(crate) fn compile_optional_take(
		&self,
		take: Option<BumpBox<'bump, Ast<'bump>>>,
	) -> Result<Option<LogicalPlan<'bump>>> {
		let Some(take_box) = take else {
			return Ok(None);
		};
		let take_ast = match BumpBox::into_inner(take_box) {
			Ast::Take(t) => t,
			_ => unreachable!("take should always be Ast::Take"),
		};
		Ok(Some(self.compile_take(take_ast)?))
	}

	fn assemble_update_pipeline(
		&self,
		from_plan: LogicalPlan<'bump>,
		filter_plan: LogicalPlan<'bump>,
		take_plan: Option<LogicalPlan<'bump>>,
		patch_plan: LogicalPlan<'bump>,
	) -> LogicalPlan<'bump> {
		let capacity = if take_plan.is_some() {
			4
		} else {
			3
		};
		let mut steps = BumpVec::with_capacity_in(capacity, self.bump);
		steps.push(from_plan);
		steps.push(filter_plan);
		if let Some(take) = take_plan {
			steps.push(take);
		}
		steps.push(patch_plan);
		LogicalPlan::Pipeline(PipelineNode {
			steps,
		})
	}

	fn wrap_update_target(
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
			return Ok(self.update_table_node(name, namespace, pipeline, returning));
		};
		let namespace_id = ns.id();

		if self.catalog.find_ringbuffer_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut t = MaybeQualifiedRingBufferIdentifier::new(name);
			if !namespace.is_empty() {
				t = t.with_namespace(namespace);
			}
			return Ok(LogicalPlan::UpdateRingBuffer(UpdateRingBufferNode {
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
			return Ok(LogicalPlan::UpdateSeries(UpdateSeriesNode {
				target: t,
				input: Some(BumpBox::new_in(pipeline, self.bump)),
				returning,
			}));
		}
		Ok(self.update_table_node(name, namespace, pipeline, returning))
	}

	#[inline]
	fn update_table_node(
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
		LogicalPlan::Update(UpdateTableNode {
			target: Some(target),
			input: Some(BumpBox::new_in(pipeline, self.bump)),
			returning,
		})
	}
}

#[inline]
fn compile_update_filter<'bump>(filter: BumpBox<'bump, Ast<'bump>>) -> Result<LogicalPlan<'bump>> {
	let filter_ast = match BumpBox::into_inner(filter) {
		Ast::Filter(f) => f,
		_ => unreachable!("filter should always be Ast::Filter"),
	};
	Ok(LogicalPlan::Filter(FilterNode {
		condition: ExpressionCompiler::compile(BumpBox::into_inner(filter_ast.node))?,
		rql: filter_ast.rql.to_string(),
	}))
}
