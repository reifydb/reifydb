// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	ast::{
		ast::{Ast, AstFrom, AstPatch, AstUpdate},
		identifier::{
			MaybeQualifiedRingBufferIdentifier, MaybeQualifiedSeriesIdentifier,
			MaybeQualifiedTableIdentifier,
		},
	},
	bump::{BumpBox, BumpVec},
	expression::ExpressionCompiler,
	plan::logical::{
		Compiler, FilterNode, LogicalPlan, PipelineNode, UpdateRingBufferNode, UpdateSeriesNode,
		UpdateTableNode,
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_update(
		&self,
		ast: AstUpdate<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		// Build internal pipeline: FROM -> FILTER -> MAP

		// 1. Create FROM scan from target
		let from_ast = AstFrom::Source {
			token: ast.token.clone(),
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
		});

		// 3. Create PATCH node from assignments (merges with original row)
		let patch_ast = AstPatch {
			token: ast.token.clone(),
			assignments: ast.assignments,
		};
		let patch_plan = self.compile_patch(patch_ast)?;

		// 4. Build pipeline: FROM -> FILTER -> PATCH
		let mut steps = BumpVec::with_capacity_in(3, self.bump);
		steps.push(from_plan);
		steps.push(filter_plan);
		steps.push(patch_plan);
		let pipeline = LogicalPlan::Pipeline(PipelineNode {
			steps,
		});

		// 5. Wrap in UPDATE node
		// Check in the catalog whether the target is a table or ring buffer
		let namespace_name = ast.target.namespace.first().map(|n| n.text().to_string());
		let namespace_name_str = namespace_name.as_deref().unwrap_or("default");
		let target_name = ast.target.name.text();
		let name = ast.target.name;
		let namespace = ast.target.namespace;

		// Try to find namespace
		let namespace_id = if let Some(ns) = self.catalog.find_namespace_by_name(tx, namespace_name_str)? {
			ns.id
		} else {
			// If namespace doesn't exist, default to table (will error during physical plan)
			let mut target = MaybeQualifiedTableIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			return Ok(LogicalPlan::Update(UpdateTableNode {
				target: Some(target),
				input: Some(BumpBox::new_in(pipeline, self.bump)),
			}));
		};

		// Check if it's a ring buffer first
		if self.catalog.find_ringbuffer_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut target = MaybeQualifiedRingBufferIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			Ok(LogicalPlan::UpdateRingBuffer(UpdateRingBufferNode {
				target,
				input: Some(BumpBox::new_in(pipeline, self.bump)),
			}))
		} else if self.catalog.find_series_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut target = MaybeQualifiedSeriesIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			Ok(LogicalPlan::UpdateSeries(UpdateSeriesNode {
				target,
				input: Some(BumpBox::new_in(pipeline, self.bump)),
			}))
		} else {
			// Assume it's a table (will error during physical plan if not found)
			let mut target = MaybeQualifiedTableIdentifier::new(name);
			if !namespace.is_empty() {
				target = target.with_namespace(namespace);
			}
			Ok(LogicalPlan::Update(UpdateTableNode {
				target: Some(target),
				input: Some(BumpBox::new_in(pipeline, self.bump)),
			}))
		}
	}
}
