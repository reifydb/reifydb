// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::Result;

use crate::{
	expression::Expression,
	flow::{
		compiler::{CompileOperator, FlowCompiler},
		operator::OperatorDef::Map,
	},
	nodes::MapNode,
	query::QueryPlan,
};

pub(crate) struct MapCompiler {
	pub input: Option<Box<QueryPlan>>,
	pub expressions: Vec<Expression>,
}

impl From<MapNode> for MapCompiler {
	fn from(node: MapNode) -> Self {
		Self {
			input: node.input,
			expressions: node.map,
		}
	}
}

impl CompileOperator for MapCompiler {
	fn compile(self, compiler: &mut FlowCompiler, txn: &mut Transaction<'_>) -> Result<OperatorId> {
		let input_node = if let Some(input) = self.input {
			Some(compiler.compile_plan(txn, *input)?)
		} else {
			None
		};

		let node_id = compiler.add_node(
			txn,
			Map {
				expressions: self.expressions,
			},
		)?;

		if let Some(input) = input_node {
			compiler.add_edge(txn, &input, &node_id)?;
		}

		Ok(node_id)
	}
}
