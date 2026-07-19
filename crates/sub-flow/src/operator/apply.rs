// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::{catalog::flow::FlowNodeId, change::Change},
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_sdk::operator::Tick;
use reifydb_value::{Result, value::duration::Duration};

use crate::{
	operator::{BoxedOperator, Operator, OperatorCell},
	transaction::FlowTransaction,
};

pub struct ApplyOperator {
	parent: OperatorCell,
	node: FlowNodeId,
	inner: BoxedOperator,
}

impl ApplyOperator {
	pub fn new(parent: OperatorCell, node: FlowNodeId, inner: BoxedOperator) -> Self {
		Self {
			parent,
			node,
			inner,
		}
	}
}

impl ApplyOperator {
	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent.output_schema()
	}
}

impl Operator for ApplyOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		self.inner.capabilities()
	}

	fn ticks(&self) -> Option<Duration> {
		self.inner.ticks()
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		self.inner.apply(txn, change)
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		self.inner.tick(txn, tick)
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.inner.sample()
	}
}
