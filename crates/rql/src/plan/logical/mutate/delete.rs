// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;

use crate::{
	ast::{
		ast::{Ast, AstDelete, AstFrom},
		identifier::{MaybeQualifiedRingBufferIdentifier, MaybeQualifiedTableIdentifier},
	},
	expression::ExpressionCompiler,
	plan::logical::{Compiler, DeleteRingBufferNode, DeleteTableNode, FilterNode, LogicalPlan, PipelineNode},
};

impl Compiler {
	pub(crate) fn compile_delete<T: AsTransaction>(
		&self,
		ast: AstDelete,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		// Build internal pipeline: FROM -> FILTER

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

		// 3. Build pipeline: FROM -> FILTER
		let pipeline = LogicalPlan::Pipeline(PipelineNode {
			steps: vec![from_plan, filter_plan],
		});

		// 4. Wrap in DELETE node
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
			return Ok(LogicalPlan::DeleteTable(DeleteTableNode {
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
			Ok(LogicalPlan::DeleteRingBuffer(DeleteRingBufferNode {
				target,
				input: Some(Box::new(pipeline)),
			}))
		} else {
			// Assume it's a table (will error during physical plan if not found)
			let mut target = MaybeQualifiedTableIdentifier::new(ast.target.name.clone());
			if let Some(ns) = ast.target.namespace.clone() {
				target = target.with_namespace(ns);
			}
			Ok(LogicalPlan::DeleteTable(DeleteTableNode {
				target: Some(target),
				input: Some(Box::new(pipeline)),
			}))
		}
	}
}
