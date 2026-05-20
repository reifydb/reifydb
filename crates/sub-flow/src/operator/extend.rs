// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_abi::operator::capabilities::CAPABILITY_ALL_STANDARD;
use reifydb_core::{
	interface::{catalog::flow::FlowNodeId, change::Change},
	value::column::columns::Columns,
};
use reifydb_rql::expression::Expression;
use reifydb_type::Result;

use crate::{Operator, operator::Operators, transaction::FlowTransaction};

pub struct ExtendOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	#[allow(dead_code)]
	expressions: Vec<Expression>,
}

impl ExtendOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, expressions: Vec<Expression>) -> Self {
		Self {
			parent,
			node,
			expressions,
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent.output_schema()
	}
}

impl Operator for ExtendOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> u32 {
		CAPABILITY_ALL_STANDARD
	}

	fn apply(&self, _txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		// TODO: Implement single-encoded extend processing

		Ok(Change::from_flow(self.node, change.version, change.diffs, change.changed_at))
	}
}
