// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_rql::{
	expression::Expression,
	nodes::{JoinInnerNode, JoinLeftNode, JoinNaturalNode},
	query::QueryPlan,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;

use super::{compile, extract_source_name_from_query};
use crate::vm::volcano::{
	join::{
		hash::{self, HashJoinNode},
		natural::NaturalJoinNode,
		nested_loop::NestedLoopJoinNode,
	},
	query::{QueryContext, QueryNode},
};

fn effective_alias(alias: Option<Fragment>, right: &QueryPlan) -> Option<Fragment> {
	let source_name = extract_source_name_from_query(right);
	alias.or(source_name).or_else(|| Some(Fragment::internal("other".to_string())))
}

fn compile_equi_or_nested<'a>(
	left: QueryPlan,
	right: Box<QueryPlan>,
	on: Vec<Expression>,
	alias: Option<Fragment>,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
	is_left_join: bool,
) -> Box<dyn QueryNode> {
	let effective_alias = effective_alias(alias, &right);
	let left_node = compile(left, rx, context.clone());
	let right_node = compile(*right, rx, context.clone());

	let analysis = hash::extract_equi_keys(&on);
	if !analysis.equi_keys.is_empty() {
		if is_left_join {
			Box::new(HashJoinNode::new_left(left_node, right_node, analysis, effective_alias))
		} else {
			Box::new(HashJoinNode::new_inner(left_node, right_node, analysis, effective_alias))
		}
	} else if is_left_join {
		Box::new(NestedLoopJoinNode::new_left(left_node, right_node, on, effective_alias))
	} else {
		Box::new(NestedLoopJoinNode::new_inner(left_node, right_node, on, effective_alias))
	}
}

pub(crate) fn compile_inner_join<'a>(
	node: JoinInnerNode,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	compile_equi_or_nested(*node.left, node.right, node.on, node.alias, rx, context, false)
}

pub(crate) fn compile_left_join<'a>(
	node: JoinLeftNode,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	compile_equi_or_nested(*node.left, node.right, node.on, node.alias, rx, context, true)
}

pub(crate) fn compile_natural_join<'a>(
	node: JoinNaturalNode,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	let effective_alias = effective_alias(node.alias, &node.right);
	let left_node = compile(*node.left, rx, context.clone());
	let right_node = compile(*node.right, rx, context.clone());
	Box::new(NaturalJoinNode::new(left_node, right_node, node.join_type, effective_alias))
}
