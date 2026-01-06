// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{interface::FlowNodeId, value::column::Columns};
use reifydb_engine::StandardColumnEvaluator;
use reifydb_sdk::FlowChange;
use reifydb_type::RowNumber;

use crate::{
	operator::{BoxedOperator, Operator, Operators},
	transaction::FlowTransaction,
};

pub struct ApplyOperator {
	parent: Arc<Operators>,
	node: FlowNodeId,
	inner: BoxedOperator,
}

impl ApplyOperator {
	pub fn new(parent: Arc<Operators>, node: FlowNodeId, inner: BoxedOperator) -> Self {
		Self {
			parent,
			node,
			inner,
		}
	}
}

impl Operator for ApplyOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		txn: &mut FlowTransaction,
		change: FlowChange,
		evaluator: &StandardColumnEvaluator,
	) -> crate::Result<FlowChange> {
		self.inner.apply(txn, change, evaluator)
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> crate::Result<Columns> {
		self.parent.pull(txn, rows)
	}
}
