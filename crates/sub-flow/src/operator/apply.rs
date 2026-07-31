// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::{catalog::flow::FlowNodeId, change::Change},
	key::operator_state::GroupSet,
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_flow::{
	operator::{Operator, Reclaimable},
	timer::Timer,
	transaction::FlowTransaction,
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};

use crate::operator::{BoxedOperator, OperatorCell};

pub struct ApplyOperator {
	parent: OperatorCell,
	node: FlowNodeId,
	inner: BoxedOperator,
	ttl: Option<Duration>,
}

impl ApplyOperator {
	pub fn new(parent: OperatorCell, node: FlowNodeId, inner: BoxedOperator, ttl: Option<Duration>) -> Self {
		Self {
			parent,
			node,
			inner,
			ttl,
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

	fn retention_scale(&self) -> Option<Duration> {
		self.inner.retention_scale().or(self.ttl)
	}

	fn invalidate_groups(&self, groups: &GroupSet) {
		self.inner.invalidate_groups(groups)
	}

	fn reclaimable_through(&self, txn: &mut FlowTransaction, watermark: DateTime) -> Result<Reclaimable> {
		let inner = self.inner.reclaimable_through(txn, watermark)?;
		if !inner.is_empty() {
			return Ok(inner);
		}
		Ok(self.ttl.map(|ttl| Reclaimable::data(watermark.saturating_sub(ttl))).unwrap_or_default())
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		self.inner.apply(txn, change)
	}

	fn on_timer(&self, txn: &mut FlowTransaction, timer: Timer) -> Result<Option<Change>> {
		self.inner.on_timer(txn, timer)
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
	use reifydb_flow::{
		operator::{Operator, Reclaimable},
		transaction::FlowTransaction,
	};
	use reifydb_runtime::sync::mutex::Mutex;
	use reifydb_value::{
		Result,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::ApplyOperator;
	use crate::operator::{OperatorCell, Operators, scale_from_millis, scan::view::SourceViewOperator};

	fn ms(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("representable duration")
	}

	fn noop_parent() -> OperatorCell {
		let view = View::Table(TableView {
			id: ViewId(1),
			namespace: NamespaceId(1),
			name: "noop".to_string(),
			kind: ViewKind::Deferred,
			columns: vec![],
			primary_key: None,
			storage: TableId(1),
			sort: vec![],
		});
		OperatorCell::new(Operators::SourceView(SourceViewOperator::new(FlowNodeId(0), view)))
	}

	struct RecordingInner {
		scale: Option<Duration>,
		frontier: Option<DateTime>,
		invalidated: Arc<Mutex<Vec<GroupId>>>,
	}

	impl RecordingInner {
		fn new(scale: Option<Duration>, invalidated: Arc<Mutex<Vec<GroupId>>>) -> Self {
			Self {
				scale,
				frontier: None,
				invalidated,
			}
		}
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

		fn retention_scale(&self) -> Option<Duration> {
			self.scale
		}

		fn reclaimable_through(&self, _txn: &mut FlowTransaction, _watermark: DateTime) -> Result<Reclaimable> {
			Ok(self.frontier.map(Reclaimable::data).unwrap_or_default())
		}

		fn invalidate_groups(&self, groups: &GroupSet) {
			self.invalidated.lock().extend_from_slice(groups.as_slice());
		}
	}

	#[test]
	fn the_apply_wrapper_reports_its_inner_operators_retention_scale() {
		// Registration sizes a node's activity grid from Operators::Apply(..).retention_scale().
		// When the wrapper swallowed it (trait default None), every windowed FFI/native
		// operator mounted under an apply node registered in the version domain (from its
		// declared ttl) while its driver stamped event-time positions - the domain-mismatch
		// panic in flow's group interner. The wrapper must hand through the inner answer,
		// both Some and None.
		let sealing = ApplyOperator::new(
			noop_parent(),
			FlowNodeId(7),
			Box::new(RecordingInner::new(Some(ms(65_000)), Arc::new(Mutex::new(Vec::new())))),
			None,
		);
		assert_eq!(sealing.retention_scale(), Some(ms(65_000)));

		let keyed = ApplyOperator::new(
			noop_parent(),
			FlowNodeId(7),
			Box::new(RecordingInner::new(None, Arc::new(Mutex::new(Vec::new())))),
			None,
		);
		assert_eq!(keyed.retention_scale(), None, "a non-sealing operator must stay in its declared domain");
	}

	#[test]
	fn an_operators_own_scale_outranks_the_declared_ttl_and_silence_defers_to_it() {
		// The operator's own scale is arithmetic it can do and a view author cannot: a 60s window
		// with 5s grace provably needs nothing older than 65s. A declared ttl is a guess at the same
		// number, so it must never win - a shorter one would silently truncate live windows.
		// Getting the precedence backwards truncates live state, or retains forever when silence
		// wins over a real ttl.
		let derived = ApplyOperator::new(
			noop_parent(),
			FlowNodeId(7),
			Box::new(RecordingInner::new(Some(ms(65_000)), Arc::new(Mutex::new(Vec::new())))),
			Some(ms(1_000)),
		);
		assert_eq!(derived.retention_scale(), Some(ms(65_000)));

		let declared = ApplyOperator::new(
			noop_parent(),
			FlowNodeId(7),
			Box::new(RecordingInner::new(None, Arc::new(Mutex::new(Vec::new())))),
			Some(ms(1_000)),
		);
		assert_eq!(declared.retention_scale(), Some(ms(1_000)));

		let neither = ApplyOperator::new(
			noop_parent(),
			FlowNodeId(7),
			Box::new(RecordingInner::new(None, Arc::new(Mutex::new(Vec::new())))),
			None,
		);
		assert_eq!(neither.retention_scale(), None, "a node declaring nothing anywhere stays perpetual");
	}

	#[test]
	fn an_unusable_guest_span_is_refused_rather_than_becoming_a_scale() {
		// A zero or unrepresentable span cannot bound anything. Refusing it lets the declared ttl
		// take over, which keeps the node aged by SOME rule; accepting it would reclaim on a
		// schedule nobody chose. Every uncertain conversion here degrades toward retaining.
		assert_eq!(scale_from_millis(Some(0)), None, "zero is not a retention scale");
		assert_eq!(scale_from_millis(None), None);
		assert_eq!(
			scale_from_millis(Some(u64::MAX)),
			None,
			"a span past the representable range must not wrap into a short scale"
		);
		assert_eq!(scale_from_millis(Some(65_000)), Some(ms(65_000)));
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
			Box::new(RecordingInner::new(None, invalidated.clone())),
			None,
		);

		apply.invalidate_groups(&GroupSet::new([GroupId(3), GroupId(9)]));

		assert_eq!(*invalidated.lock(), vec![GroupId(3), GroupId(9)]);
	}
}
