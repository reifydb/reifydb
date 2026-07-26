// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::{catalog::flow::FlowNodeId, change::Change},
	key::operator_state::GroupSet,
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
use reifydb_sdk::operator::Tick;
use reifydb_value::{Result, value::duration::Duration};

use crate::operator::{BoxedOperator, OperatorCell};

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

	fn seal_after_ms(&self) -> Option<u64> {
		self.inner.seal_after_ms()
	}

	fn invalidate_groups(&self, groups: &GroupSet) {
		self.inner.invalidate_groups(groups)
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

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_abi::operator::capabilities::OperatorCapability;
	use reifydb_core::{
		interface::{
			catalog::{
				flow::FlowNodeId,
				id::{NamespaceId, TableId, ViewId},
				view::{TableView, View, ViewKind},
			},
			change::Change,
		},
		key::operator_state::{GroupId, GroupSet},
	};
	use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
	use reifydb_runtime::sync::mutex::Mutex;
	use reifydb_value::Result;

	use super::ApplyOperator;
	use crate::operator::{OperatorCell, Operators, scan::view::PrimitiveViewOperator};

	fn noop_parent() -> OperatorCell {
		let view = View::Table(TableView {
			id: ViewId(1),
			namespace: NamespaceId(1),
			name: "noop".to_string(),
			kind: ViewKind::Deferred,
			columns: vec![],
			primary_key: None,
			underlying: TableId(1),
			sort: vec![],
		});
		OperatorCell::new(Operators::SourceView(PrimitiveViewOperator::new(FlowNodeId(0), view)))
	}

	struct RecordingInner {
		seal_after: Option<u64>,
		invalidated: Arc<Mutex<Vec<GroupId>>>,
	}

	impl Operator for RecordingInner {
		fn id(&self) -> FlowNodeId {
			FlowNodeId(7)
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			&[]
		}

		fn apply(&self, _txn: &mut FlowTransaction, change: Change) -> Result<Change> {
			Ok(change)
		}

		fn seal_after_ms(&self) -> Option<u64> {
			self.seal_after
		}

		fn invalidate_groups(&self, groups: &GroupSet) {
			self.invalidated.lock().extend_from_slice(groups.as_slice());
		}
	}

	#[test]
	fn the_apply_wrapper_reports_its_inner_operators_seal_span() {
		// Registration derives a node's horizon from Operators::Apply(..).seal_after_ms().
		// When the wrapper swallowed it (trait default None), every windowed FFI/native
		// operator mounted under an apply node registered in the version domain (from its
		// declared ttl) while its driver stamped event-time positions - the domain-mismatch
		// panic in flow's group interner. The wrapper must hand through the inner answer,
		// both Some and None.
		let sealing = ApplyOperator::new(
			noop_parent(),
			FlowNodeId(7),
			Box::new(RecordingInner {
				seal_after: Some(65_000),
				invalidated: Arc::new(Mutex::new(Vec::new())),
			}),
		);
		assert_eq!(sealing.seal_after_ms(), Some(65_000));

		let keyed = ApplyOperator::new(
			noop_parent(),
			FlowNodeId(7),
			Box::new(RecordingInner {
				seal_after: None,
				invalidated: Arc::new(Mutex::new(Vec::new())),
			}),
		);
		assert_eq!(keyed.seal_after_ms(), None, "a non-sealing operator must stay in its declared domain");
	}

	#[test]
	fn reclaimed_groups_reach_the_operator_behind_the_apply_wrapper() {
		// The reclaim driver erases group state on disk and then calls invalidate_groups so
		// the operator drops its RAM copies. When the wrapper swallowed the call (trait
		// default no-op), native operators kept serving ghost rows for groups whose durable
		// state was already gone.
		let invalidated = Arc::new(Mutex::new(Vec::new()));
		let apply = ApplyOperator::new(
			noop_parent(),
			FlowNodeId(7),
			Box::new(RecordingInner {
				seal_after: None,
				invalidated: invalidated.clone(),
			}),
		);

		apply.invalidate_groups(&GroupSet::new([GroupId(3), GroupId(9)]));

		assert_eq!(*invalidated.lock(), vec![GroupId(3), GroupId(9)]);
	}
}
