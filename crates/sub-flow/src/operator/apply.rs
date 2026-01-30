// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{interface::catalog::flow::FlowNodeId, value::column::columns::Columns};
use reifydb_engine::evaluate::column::StandardColumnEvaluator;
use reifydb_core::interface::change::Change;
use reifydb_type::value::row_number::RowNumber;

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
		change: Change,
		evaluator: &StandardColumnEvaluator,
	) -> reifydb_type::Result<Change> {
		self.inner.apply(txn, change, evaluator)
	}

	fn pull(&self, txn: &mut FlowTransaction, rows: &[RowNumber]) -> reifydb_type::Result<Columns> {
		self.parent.pull(txn, rows)
	}
}
