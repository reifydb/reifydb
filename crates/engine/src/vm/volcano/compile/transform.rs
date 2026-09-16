// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_rql::{
	nodes::{
		ExtendNode as RqlExtendNode, FilterNode as RqlFilterNode, MapNode as RqlMapNode,
		PatchNode as RqlPatchNode,
	},
	query::extract_resolved_source,
};
use reifydb_transaction::transaction::Transaction;

use super::compile;
use crate::vm::volcano::{
	extend::{ExtendNode, ExtendWithoutInputNode},
	filter::FilterNode,
	map::{MapNode, MapWithoutInputNode},
	patch::PatchNode,
	query::{QueryContext, QueryNode},
};

pub(crate) fn compile_filter<'a>(
	node: RqlFilterNode,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	let source = extract_resolved_source(&node.input);
	let input_node = compile(*node.input, rx, context);
	Box::new(FilterNode::with_source(input_node, node.conditions, source))
}

pub(crate) fn compile_map<'a>(
	node: RqlMapNode,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	if let Some(input) = node.input {
		let source = extract_resolved_source(&input);
		let input_node = compile(*input, rx, context);
		Box::new(MapNode::new(input_node, node.map, source))
	} else {
		Box::new(MapWithoutInputNode::new(node.map))
	}
}

pub(crate) fn compile_extend<'a>(
	node: RqlExtendNode,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	if let Some(input) = node.input {
		let source = extract_resolved_source(&input);
		let input_node = compile(*input, rx, context);
		Box::new(ExtendNode::new(input_node, node.extend, source))
	} else {
		Box::new(ExtendWithoutInputNode::new(node.extend))
	}
}

pub(crate) fn compile_patch<'a>(
	node: RqlPatchNode,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	let input = node.input.expect("Patch requires input");
	let source = extract_resolved_source(&input);
	let input_node = compile(*input, rx, context);
	Box::new(PatchNode::new(input_node, node.assignments, source))
}
