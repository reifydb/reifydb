// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{cell::RefCell, time::Duration};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	encoded::key::EncodedKey,
	interface::{catalog::flow::FlowNodeId, change::Change},
	row::TtlAnchor,
	value::column::columns::Columns,
};
use reifydb_sdk::operator::Tick;
use reifydb_type::Result;

use crate::{
	operator::{BoxedOperator, Operator, OperatorCell, stateful::utils::evict_state_by_ttl},
	transaction::FlowTransaction,
};

pub struct ApplyOperator {
	parent: OperatorCell,
	node: FlowNodeId,
	inner: BoxedOperator,
	ttl_nanos: Option<u64>,
	ttl_anchor: TtlAnchor,
	evict_cursor: RefCell<Option<EncodedKey>>,
	capabilities: Box<[OperatorCapability]>,
}

impl ApplyOperator {
	pub fn new(
		parent: OperatorCell,
		node: FlowNodeId,
		inner: BoxedOperator,
		ttl_nanos: Option<u64>,
		ttl_anchor: TtlAnchor,
	) -> Self {
		let mut capabilities: Vec<OperatorCapability> = inner.capabilities().to_vec();
		if ttl_nanos.is_some() && !capabilities.contains(&OperatorCapability::Tick) {
			capabilities.push(OperatorCapability::Tick);
		}
		Self {
			parent,
			node,
			inner,
			ttl_nanos,
			ttl_anchor,
			evict_cursor: RefCell::new(None),
			capabilities: capabilities.into_boxed_slice(),
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
		&self.capabilities
	}

	fn ticks(&self) -> Option<Duration> {
		let inner = self.inner.ticks();
		let evict = self.ttl_nanos.map(|_| Duration::from_secs(1));
		match (inner, evict) {
			(Some(a), Some(b)) => Some(a.min(b)),
			(Some(a), None) => Some(a),
			(None, other) => other,
		}
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		self.inner.apply(txn, change)
	}

	fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		let now_nanos = tick.now.to_nanos();
		let inner_change = if self.inner.ticks().is_some() {
			self.inner.tick(txn, tick)?
		} else {
			None
		};
		if let Some(ttl_nanos) = self.ttl_nanos {
			let mut cursor = self.evict_cursor.borrow_mut();
			evict_state_by_ttl(self.node, txn, ttl_nanos, self.ttl_anchor, now_nanos, &mut cursor)?;
		}
		Ok(inner_change)
	}
}
