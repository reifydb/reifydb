// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change},
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_flow::{
	operator::{BoxedOperator, Operator},
	timer::Timer,
	transaction::FlowTransaction,
};
use reifydb_store_operator::{CompactionOutcome, FloorSpec};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};

use crate::operator::{OperatorCell, max_input_time, stamp_output_time};

pub struct ApplyOperator {
	parent: OperatorCell,
	operator: OperatorId,
	inner: BoxedOperator,
	ttl: Option<Duration>,
}

impl ApplyOperator {
	pub fn new(parent: OperatorCell, operator: OperatorId, inner: BoxedOperator, ttl: Option<Duration>) -> Self {
		Self {
			parent,
			operator,
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
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		self.inner.capabilities()
	}

	fn retention_scale(&self) -> Option<Duration> {
		self.inner.retention_scale().or(self.ttl)
	}

	fn floors(&self, txn: &mut FlowTransaction, watermark: DateTime) -> Result<FloorSpec> {
		let inner = self.inner.floors(txn, watermark)?;
		if !inner.is_empty() {
			return Ok(inner);
		}
		Ok(self.ttl.map(|ttl| FloorSpec::data(watermark.saturating_sub(ttl))).unwrap_or_default())
	}

	fn on_compacted(&self, outcome: &CompactionOutcome) {
		self.inner.on_compacted(outcome)
	}

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		let inherited = max_input_time(&change);
		let mut out = self.inner.apply(txn, change)?;
		stamp_output_time(&mut out, inherited);
		Ok(out)
	}

	fn on_timer(&self, txn: &mut FlowTransaction, timer: Timer) -> Result<Option<Change>> {
		let at = timer.at;
		let mut out = self.inner.on_timer(txn, timer)?;
		if let Some(change) = out.as_mut() {
			stamp_output_time(change, Some(at));
		}
		Ok(out)
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.inner.sample()
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_abi::operator::capabilities::OperatorCapability;
	use reifydb_core::interface::{
		catalog::{
			flow::OperatorId,
			id::{NamespaceId, TableId, ViewId},
			view::{TableView, View, ViewKind},
		},
		change::Change,
	};
	use reifydb_flow::{operator::Operator, transaction::FlowTransaction};
	use reifydb_store_operator::FloorSpec;
	use reifydb_value::{
		Result,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::ApplyOperator;
	use crate::operator::{OperatorCell, scale_from_millis, scan::view::SourceViewOperator};

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
		OperatorCell::new(SourceViewOperator::new(OperatorId(0), view))
	}

	struct RecordingInner {
		scale: Option<Duration>,
		floor: Option<DateTime>,
	}

	impl RecordingInner {
		fn new(scale: Option<Duration>) -> Self {
			Self {
				scale,
				floor: None,
			}
		}

		fn with_floor(scale: Option<Duration>, floor: DateTime) -> Self {
			Self {
				scale,
				floor: Some(floor),
			}
		}
	}

	impl Operator for RecordingInner {
		fn id(&self) -> OperatorId {
			OperatorId(7)
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

		fn floors(&self, _txn: &mut FlowTransaction, _watermark: DateTime) -> Result<FloorSpec> {
			Ok(self.floor.map(FloorSpec::data).unwrap_or_default())
		}
	}

	#[test]
	fn the_apply_wrapper_reports_its_inner_operators_retention_scale() {
		// Registration sizes the operator's activity grid from this answer, so a wrapper that
		// swallowed it would register a windowed guest in the version domain while its driver
		// stamps event-time positions - a domain mismatch in the group interner.
		let sealing = ApplyOperator::new(
			noop_parent(),
			OperatorId(7),
			Box::new(RecordingInner::new(Some(ms(65_000)))),
			None,
		);
		assert_eq!(sealing.retention_scale(), Some(ms(65_000)));

		let keyed = ApplyOperator::new(noop_parent(), OperatorId(7), Box::new(RecordingInner::new(None)), None);
		assert_eq!(keyed.retention_scale(), None, "a non-sealing operator must stay in its declared domain");
	}

	#[test]
	fn an_operators_own_scale_outranks_the_declared_ttl_and_silence_defers_to_it() {
		// An operator derives its scale exactly (a 60s window with 5s grace needs nothing older
		// than 65s) while a declared ttl only guesses at it, so a shorter declaration must never
		// win and truncate live windows.
		let derived = ApplyOperator::new(
			noop_parent(),
			OperatorId(7),
			Box::new(RecordingInner::new(Some(ms(65_000)))),
			Some(ms(1_000)),
		);
		assert_eq!(derived.retention_scale(), Some(ms(65_000)));

		let declared = ApplyOperator::new(
			noop_parent(),
			OperatorId(7),
			Box::new(RecordingInner::new(None)),
			Some(ms(1_000)),
		);
		assert_eq!(declared.retention_scale(), Some(ms(1_000)));

		let neither =
			ApplyOperator::new(noop_parent(), OperatorId(7), Box::new(RecordingInner::new(None)), None);
		assert_eq!(neither.retention_scale(), None, "a operator declaring nothing anywhere stays perpetual");
	}

	#[test]
	fn an_unusable_guest_span_is_refused_rather_than_becoming_a_scale() {
		// Refusing an unusable span lets the declared ttl take over, so the operator is still aged by
		// some rule; accepting it would reclaim on a schedule nobody chose.
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
	fn the_inner_operators_floors_outrank_the_declared_ttl_and_silence_defers_to_it() {
		// The inner operator derives its floors exactly (a sealing guest anchors on its own seal
		// ledger); the declared ttl is a fallback for operators that derive nothing. A wrapper that
		// preferred the ttl would floor a sealing guest's live windows.
		// Mutation falsified against: swapping the precedence (ttl would win, floor != anchor) and
		// dropping the fallback (declared-only guest would return an empty spec).
		use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};

		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().deferred();
		let watermark = DateTime::from_millis(1_000_000);
		let anchor = DateTime::from_millis(400_000);

		let derived = ApplyOperator::new(
			noop_parent(),
			OperatorId(7),
			Box::new(RecordingInner::with_floor(Some(ms(65_000)), anchor)),
			Some(ms(1_000)),
		);
		assert_eq!(derived.floors(&mut txn, watermark).unwrap().data_cutoff(), Some(anchor));

		let declared = ApplyOperator::new(
			noop_parent(),
			OperatorId(7),
			Box::new(RecordingInner::new(None)),
			Some(ms(1_000)),
		);
		assert_eq!(
			declared.floors(&mut txn, watermark).unwrap().data_cutoff(),
			Some(DateTime::from_millis(999_000)),
			"a silent inner operator defers to the declared ttl"
		);

		let neither =
			ApplyOperator::new(noop_parent(), OperatorId(7), Box::new(RecordingInner::new(None)), None);
		assert!(neither.floors(&mut txn, watermark).unwrap().is_empty());
	}
}
