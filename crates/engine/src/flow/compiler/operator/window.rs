// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::{WindowSize, WindowSlide, WindowType},
	interface::catalog::flow::FlowNodeId,
};
use reifydb_rql::{
	expression::Expression,
	flow::{
		conversion::{to_owned_expressions, to_owned_physical_plan},
		node::FlowNodeType::Window,
	},
	plan::physical::{PhysicalPlan, WindowNode},
};
use reifydb_transaction::standard::command::StandardCommandTransaction;
use reifydb_type::Result;

use crate::flow::compiler::{CompileOperator, FlowCompiler};

pub(crate) struct WindowCompiler {
	pub input: Option<Box<PhysicalPlan>>,
	pub window_type: WindowType,
	pub size: WindowSize,
	pub slide: Option<WindowSlide>,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub min_events: usize,
	pub max_window_count: Option<usize>,
	pub max_window_age: Option<std::time::Duration>,
}

impl From<WindowNode> for WindowCompiler {
	fn from(node: WindowNode) -> Self {
		Self {
			input: node.input.map(|input| Box::new(to_owned_physical_plan(*input))),
			window_type: node.window_type,
			size: node.size,
			slide: node.slide,
			group_by: to_owned_expressions(node.group_by),
			aggregations: to_owned_expressions(node.aggregations),
			min_events: node.min_events,
			max_window_count: node.max_window_count,
			max_window_age: node.max_window_age,
		}
	}
}

impl CompileOperator for WindowCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut StandardCommandTransaction) -> Result<FlowNodeId> {
		// Compile input first if present
		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(txn, *input)?)
		} else {
			None
		};

		let node_id = compiler.add_node(
			txn,
			Window {
				window_type: self.window_type,
				size: self.size,
				slide: self.slide,
				group_by: self.group_by,
				aggregations: self.aggregations,
				min_events: self.min_events,
				max_window_count: self.max_window_count,
				max_window_age: self.max_window_age,
			},
		)?;

		// Add input edge if we have one
		if let Some(input_node) = input_node {
			compiler.add_edge(txn, &input_node, &node_id)?;
		}

		Ok(node_id)
	}
}
