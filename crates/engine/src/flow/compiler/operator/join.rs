// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::JoinType::{self, Inner, Left},
	interface::catalog::flow::FlowNodeId,
};
use reifydb_rql::{
	expression::Expression,
	flow::node::FlowNodeType,
	nodes::{JoinInnerNode, JoinLeftNode},
	query::QueryPlan,
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct JoinCompiler {
	pub join_type: JoinType,
	pub left: Box<QueryPlan>,
	pub right: Box<QueryPlan>,
	pub on: Vec<Expression>,
	pub alias: Option<String>,
}

impl From<JoinInnerNode> for JoinCompiler {
	fn from(node: JoinInnerNode) -> Self {
		Self {
			join_type: Inner,
			left: node.left,
			right: node.right,
			on: node.on,
			alias: node.alias.map(|f| f.text().to_string()),
		}
	}
}

impl From<JoinLeftNode> for JoinCompiler {
	fn from(node: JoinLeftNode) -> Self {
		Self {
			join_type: Left,
			left: node.left,
			right: node.right,
			on: node.on,
			alias: node.alias.map(|f| f.text().to_string()),
		}
	}
}

// Extract the source name from a query plan if it's a scan node
fn extract_source_name(plan: &QueryPlan) -> Option<String> {
	match plan {
		QueryPlan::TableScan(node) => Some(node.source.def().name.clone()),
		QueryPlan::ViewScan(node) => Some(node.source.def().name.clone()),
		QueryPlan::RingBufferScan(node) => Some(node.source.def().name.clone()),
		QueryPlan::DictionaryScan(node) => Some(node.source.def().name.clone()),
		// For other node types, try to recursively find the source
		QueryPlan::Filter(node) => extract_source_name(&node.input),
		QueryPlan::Map(node) => node.input.as_ref().and_then(|p| extract_source_name(p)),
		QueryPlan::Take(node) => extract_source_name(&node.input),
		_ => None,
	}
}

// Extract the left and right column references from join conditions
fn extract_join_keys(conditions: &[Expression]) -> (Vec<Expression>, Vec<Expression>) {
	let mut left_keys = Vec::new();
	let mut right_keys = Vec::new();

	for condition in conditions {
		match condition {
			Expression::Equal(eq) => {
				// For equality conditions, extract the left and
				// right expressions
				left_keys.push(*eq.left.clone());
				right_keys.push(*eq.right.clone());
			}
			// For now, we only support simple equality joins
			// More complex conditions could be added later
			_ => {
				// If it's not an equality, we'll add the whole
				// condition to both sides This maintains
				// backwards compatibility but may not work
				// correctly
				left_keys.push(condition.clone());
				right_keys.push(condition.clone());
			}
		}
	}

	(left_keys, right_keys)
}

impl CompileOperator for JoinCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut AdminTransaction) -> Result<FlowNodeId> {
		// Extract source name from right plan for fallback alias
		let source_name = extract_source_name(&self.right);

		let left_node = compiler.compile_plan(txn, *self.left)?;
		let right_node = compiler.compile_plan(txn, *self.right)?;

		let (left_keys, right_keys) = extract_join_keys(&self.on);

		// Use explicit alias, or fall back to extracted source name, or use "other"
		let effective_alias = self.alias.or(source_name).or_else(|| Some("other".to_string()));

		let node_id = compiler.add_node(
			txn,
			FlowNodeType::Join {
				join_type: self.join_type,
				left: left_keys,
				right: right_keys,
				alias: effective_alias,
			},
		)?;

		compiler.add_edge(txn, &left_node, &node_id)?;
		compiler.add_edge(txn, &right_node, &node_id)?;

		Ok(node_id)
	}
}
