// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
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
	) -> crate::Result<LogicalPlan<'bump>> {
		// Build internal pipeline: FROM -> FILTER

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

		// 3. Build pipeline: FROM -> FILTER
		let mut steps = BumpVec::with_capacity_in(2, self.bump);
		steps.push(from_plan);
		steps.push(filter_plan);
		let pipeline = LogicalPlan::Pipeline(PipelineNode {
			steps,
		});

		// 4. Wrap in DELETE node
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
			return Ok(LogicalPlan::DeleteTable(DeleteTableNode {
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
			return Ok(LogicalPlan::DeleteRingBuffer(DeleteRingBufferNode {
				target,
				input: Some(BumpBox::new_in(pipeline, self.bump)),
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
		}))
	}
}
