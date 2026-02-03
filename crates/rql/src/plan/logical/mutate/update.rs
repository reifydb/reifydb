// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;

use crate::{
	ast::{
		ast::{Ast, AstFrom, AstPatch, AstUpdate},
		identifier::{MaybeQualifiedRingBufferIdentifier, MaybeQualifiedTableIdentifier},
	},
	expression::ExpressionCompiler,
	plan::logical::{Compiler, FilterNode, LogicalPlan, PipelineNode, UpdateRingBufferNode, UpdateTableNode},
};

impl Compiler {
	pub(crate) fn compile_update<T: AsTransaction>(
		&self,
		ast: AstUpdate,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		// Build internal pipeline: FROM -> FILTER -> MAP

		// 1. Create FROM scan from target
		let from_ast = AstFrom::Source {
			token: ast.token.clone(),
			source: ast.target.clone(),
			index_name: None,
		};
		let from_plan = self.compile_from(from_ast, tx)?;

		// 2. Create FILTER node from the filter clause
		let filter_ast = match *ast.filter {
			Ast::Filter(f) => f,
			_ => unreachable!("filter should always be Ast::Filter"),
		};
		let filter_plan = LogicalPlan::Filter(FilterNode {
			condition: ExpressionCompiler::compile(*filter_ast.node)?,
		});

		// 3. Create PATCH node from assignments (merges with original row)
		let patch_ast = AstPatch {
			token: ast.token.clone(),
			assignments: ast.assignments,
		};
		let patch_plan = self.compile_patch(patch_ast)?;

		// 4. Build pipeline: FROM -> FILTER -> PATCH
		let pipeline = LogicalPlan::Pipeline(PipelineNode {
			steps: vec![from_plan, filter_plan, patch_plan],
		});

		// 5. Wrap in UPDATE node
		// Check in the catalog whether the target is a table or ring buffer
		let namespace_name = ast.target.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
		let target_name = ast.target.name.text();

		// Try to find namespace
		let namespace_id = if let Some(ns) = self.catalog.find_namespace_by_name(tx, namespace_name)? {
			ns.id
		} else {
			// If namespace doesn't exist, default to table (will error during physical plan)
			let mut target = MaybeQualifiedTableIdentifier::new(ast.target.name.clone());
			if let Some(ns) = ast.target.namespace.clone() {
				target = target.with_namespace(ns);
			}
			return Ok(LogicalPlan::Update(UpdateTableNode {
				target: Some(target),
				input: Some(Box::new(pipeline)),
			}));
		};

		// Check if it's a ring buffer first
		if self.catalog.find_ringbuffer_by_name(tx, namespace_id, target_name)?.is_some() {
			let mut target = MaybeQualifiedRingBufferIdentifier::new(ast.target.name.clone());
			if let Some(ns) = ast.target.namespace.clone() {
				target = target.with_namespace(ns);
			}
			Ok(LogicalPlan::UpdateRingBuffer(UpdateRingBufferNode {
				target,
				input: Some(Box::new(pipeline)),
			}))
		} else {
			// Assume it's a table (will error during physical plan if not found)
			let mut target = MaybeQualifiedTableIdentifier::new(ast.target.name.clone());
			if let Some(ns) = ast.target.namespace.clone() {
				target = target.with_namespace(ns);
			}
			Ok(LogicalPlan::Update(UpdateTableNode {
				target: Some(target),
				input: Some(Box::new(pipeline)),
			}))
		}
	}
}
