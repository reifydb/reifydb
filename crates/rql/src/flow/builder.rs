// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Builder pattern for constructing flow nodes with edges

use reifydb_core::interface::{CommandTransaction, FlowNodeId};

use super::{FlowCompiler, FlowNodeType};
use crate::Result;

/// Builder for creating flow nodes with automatic edge management
pub(crate) struct FlowNodeBuilder<'a, T>
where
	T: CommandTransaction,
{
	compiler: &'a mut FlowCompiler<T>,
	node_type: FlowNodeType,
	input_nodes: Vec<FlowNodeId>,
}

impl<'a, T> FlowNodeBuilder<'a, T>
where
	T: CommandTransaction,
{
	/// Creates a new FlowNodeBuilder
	pub fn new(compiler: &'a mut FlowCompiler<T>, node_type: FlowNodeType) -> Self {
		Self {
			compiler,
			node_type,
			input_nodes: Vec::new(),
		}
	}

	/// Adds an input operator to connect to this operator
	pub fn with_input(mut self, input: FlowNodeId) -> Self {
		self.input_nodes.push(input);
		self
	}

	/// Adds multiple input nodes to connect to this operator
	pub fn with_inputs(mut self, inputs: impl IntoIterator<Item = FlowNodeId>) -> Self {
		self.input_nodes.extend(inputs);
		self
	}

	/// Builds the operator and creates all edges
	pub async fn build(self) -> Result<FlowNodeId> {
		// Create the operator
		let node_id = self.compiler.add_node(self.node_type).await?;

		// Add edges from all input nodes to this operator
		for input in self.input_nodes {
			self.compiler.add_edge(&input, &node_id).await?;
		}

		Ok(node_id)
	}
}

/// Extension trait to provide builder methods on FlowCompiler
impl<T> FlowCompiler<T>
where
	T: CommandTransaction,
{
	/// Creates a new FlowNodeBuilder for this compiler
	pub(crate) fn build_node(&mut self, node_type: FlowNodeType) -> FlowNodeBuilder<'_, T> {
		FlowNodeBuilder::new(self, node_type)
	}
}
