// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	ast::{
		ast::{Ast, AstDelete, AstFrom},
		identifier::{
			MaybeQualifiedRingBufferIdentifier, MaybeQualifiedSeriesIdentifier,
			MaybeQualifiedTableIdentifier,
		},
	},
	bump::{BumpBox, BumpVec},
	expression::ExpressionCompiler,
	plan::logical::{
		Compiler, DeleteRingBufferNode, DeleteSeriesNode, DeleteTableNode, FilterNode, LogicalPlan,
		PipelineNode,
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_delete(
		&self,
		ast: AstDelete<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let returning = if let Some(returning_asts) = ast.returning {
			let mut exprs = Vec::with_capacity(returning_asts.len());
			for ast_node in returning_asts {
				exprs.push(ExpressionCompiler::compile(ast_node)?);
			}
			Some(exprs)
		} else {
			None
		};

		// Build internal pipeline: FROM -> FILTER

		// 1. Create FROM scan from target
		let from_ast = AstFrom::Source {
			token: ast.token,
			source: ast.target.clone(),
			index_name: None,
		};
		let from_plan = self.compile_from(from_ast, tx)?;

		// 2. Create FILTER node from the filter clause
		let filter_ast = match BumpBox::into_inner(ast.filter) {
			Ast::Filter(f) => f,
			_ => unreachable!("filter should always be Ast::Filter"),
		};
		let filter_plan = LogicalPlan::Filter(FilterNode {
			condition: ExpressionCompiler::compile(BumpBox::into_inner(filter_ast.node))?,
			rql: filter_ast.rql.to_string(),
		});

		// 3. Build pipeline: FROM -> FILTER -> [TAKE]
		let take_plan = if let Some(take_box) = ast.take {
			let take_ast = match BumpBox::into_inner(take_box) {
				Ast::Take(t) => t,
				_ => unreachable!("take should always be Ast::Take"),
			};
			Some(self.compile_take(take_ast)?)
		} else {
			None
		};

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
		let pipeline = LogicalPlan::Pipeline(PipelineNode {
			steps,
		});

		// 4. Wrap in DELETE node
		// Check in the catalog whether the target is a table or ring buffer
		let target_name = ast.target.name.text();
		let name = ast.target.name;
		let namespace = ast.target.namespace;
		let ns_segments: Vec<&str> = namespace.iter().map(|n| n.text()).collect();

		// Try to find namespace
		let namespace_id = if let Some(ns) = self.catalog.find_namespace_by_segments(tx, &ns_segments)? {
			ns.id()
		} else {
			// If namespace doesn't exist, default to table (will error during physical plan)
			let mut target = MaybeQualifiedTableIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			return Ok(LogicalPlan::DeleteTable(DeleteTableNode {
				target: Some(target),
				input: Some(BumpBox::new_in(pipeline, self.bump)),
				returning,
			}));
		};

		// Check if it's a ring buffer first
		if self.catalog.find_ringbuffer_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut target = MaybeQualifiedRingBufferIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			return Ok(LogicalPlan::DeleteRingBuffer(DeleteRingBufferNode {
				target,
				input: Some(BumpBox::new_in(pipeline, self.bump)),
				returning,
			}));
		}

		// Check if it's a series
		if self.catalog.find_series_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut target = MaybeQualifiedSeriesIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			return Ok(LogicalPlan::DeleteSeries(DeleteSeriesNode {
				target,
				input: Some(BumpBox::new_in(pipeline, self.bump)),
				returning,
			}));
		}

		// Assume it's a table (will error during physical plan if not found)
		let mut target = MaybeQualifiedTableIdentifier::new(name);
		if !namespace.is_empty() {
			target = target.with_namespace(namespace);
		}
		Ok(LogicalPlan::DeleteTable(DeleteTableNode {
			target: Some(target),
			input: Some(BumpBox::new_in(pipeline, self.bump)),
			returning,
		}))
	}
}
