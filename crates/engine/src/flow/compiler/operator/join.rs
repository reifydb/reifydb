// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::store::ttl::create::create_operator_ttl;
use reifydb_core::{
	common::JoinType::{self, Inner, Left},
	interface::catalog::flow::FlowNodeId,
	row::Ttl,
};
use reifydb_rql::{
	expression::Expression,
	flow::node::FlowNodeType,
	nodes::{JoinInnerNode, JoinLeftNode},
	query::QueryPlan,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct JoinCompiler {
	pub join_type: JoinType,
	pub left: Box<QueryPlan>,
	pub right: Box<QueryPlan>,
	pub on: Vec<Expression>,
	pub alias: Option<String>,
	pub ttl: Option<Ttl>,
}

impl From<JoinInnerNode> for JoinCompiler {
	fn from(node: JoinInnerNode) -> Self {
		Self {
			join_type: Inner,
			left: node.left,
			right: node.right,
			on: node.on,
			alias: node.alias.map(|f| f.text().to_string()),
			ttl: node.ttl,
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
			ttl: node.ttl,
		}
	}
}

// Extract the source name from a query plan if it's a scan node
fn extract_source_name(plan: &QueryPlan) -> Option<String> {
	match plan {
		QueryPlan::TableScan(node) => Some(node.source.def().name.clone()),
		QueryPlan::ViewScan(node) => Some(node.source.def().name().to_string()),
		QueryPlan::RingBufferScan(node) => Some(node.source.def().name.clone()),
		QueryPlan::DictionaryScan(node) => Some(node.source.def().name.clone()),
		// For other node types, try to recursively find the source
		QueryPlan::Filter(node) => extract_source_name(&node.input),
		QueryPlan::Map(node) => node.input.as_ref().and_then(|p| extract_source_name(p)),
		QueryPlan::Take(node) => extract_source_name(&node.input),
		_ => None,
	}
}

/// Recursively collect all Equal leaves from an And tree.
fn collect_equal_conditions(expr: &Expression, out: &mut Vec<Expression>) {
	match expr {
		Expression::And(and) => {
			collect_equal_conditions(&and.left, out);
			collect_equal_conditions(&and.right, out);
		}
		other => out.push(other.clone()),
	}
}

/// Extract left and right key expressions from join conditions.
/// Handles multi-column joins where conditions are combined with And.
fn extract_join_keys(conditions: &[Expression]) -> (Vec<Expression>, Vec<Expression>) {
	let mut left_keys = Vec::new();
	let mut right_keys = Vec::new();

	// Flatten any And trees into individual conditions
	let mut flat = Vec::new();
	for condition in conditions {
		collect_equal_conditions(condition, &mut flat);
	}

	for condition in flat {
		match condition {
			Expression::Equal(eq) => {
				left_keys.push(*eq.left.clone());
				right_keys.push(*eq.right.clone());
			}
			_ => {
				// Non-equality condition: pass through to both sides (existing fallback)
				left_keys.push(condition.clone());
				right_keys.push(condition.clone());
			}
		}
	}

	(left_keys, right_keys)
}

impl CompileOperator for JoinCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<FlowNodeId> {
		// Extract source name from right plan for fallback alias
		let source_name = extract_source_name(&self.right);

		let left_node = compiler.compile_plan(txn, *self.left)?;
		let right_node = compiler.compile_plan(txn, *self.right)?;

		let (left_keys, right_keys) = extract_join_keys(&self.on);

		// Use explicit alias, or fall back to extracted source name, or use "other"
		let effective_alias = self.alias.or(source_name).or_else(|| Some("other".to_string()));

		let ttl = self.ttl.clone();
		let node_id = compiler.add_node(
			txn,
			FlowNodeType::Join {
				join_type: self.join_type,
				left: left_keys,
				right: right_keys,
				alias: effective_alias,
				ttl: self.ttl,
			},
		)?;

		if let Some(ttl) = ttl
			&& let Transaction::Admin(admin) = txn
		{
			create_operator_ttl(admin, node_id, &ttl)?;
		}

		compiler.add_edge(txn, &left_node, &node_id)?;
		compiler.add_edge(txn, &right_node, &node_id)?;

		Ok(node_id)
	}
}
