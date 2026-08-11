// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_value::{Result, value::duration::Duration};

use crate::{
	operator::{BoxedOperator, Operator, OperatorCell, max_input_time, stamp_output_time},
	timer::Timer,
	transaction::interface::FlowTransaction,
};

pub struct ApplyOperator<T: FlowTransaction> {
	parent: OperatorCell<T>,
	operator: OperatorId,
	inner: BoxedOperator<T>,
	_ttl: Option<Duration>,
}

impl<T: FlowTransaction> ApplyOperator<T> {
	pub fn new(
		parent: OperatorCell<T>,
		operator: OperatorId,
		inner: BoxedOperator<T>,
		ttl: Option<Duration>,
	) -> Self {
		Self {
			parent,
			operator,
			inner,
			_ttl: ttl,
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent.output_schema()
	}
}

impl<T: FlowTransaction> Operator<T> for ApplyOperator<T> {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		self.inner.capabilities()
	}

	fn seal_span(&self) -> Option<Duration> {
		self.inner.seal_span()
	}

	fn apply(&self, txn: &mut T, change: Change) -> Result<Change> {
		let inherited = max_input_time(&change);
		let mut out = self.inner.apply(txn, change)?;
		stamp_output_time(&mut out, inherited);
		Ok(out)
	}

	fn on_timer(&self, txn: &mut T, timer: Timer) -> Result<Option<Change>> {
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
	use reifydb_core::interface::{
		catalog::{
			flow::OperatorId,
			id::{NamespaceId, TableId, ViewId},
			view::{TableView, View, ViewKind},
		},
		change::Change,
		flow::OperatorCapability,
	};
	use reifydb_value::{Result, value::duration::Duration};

	use super::ApplyOperator;
	use crate::{
		operator::{Operator, OperatorCell, scale_from_millis, scan::view::SourceViewOperator},
		transaction::deferred::DeferredTransaction,
	};

	fn ms(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("representable duration")
	}

	fn noop_parent() -> OperatorCell<DeferredTransaction> {
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

	#[test]
	fn an_unusable_guest_span_is_refused_rather_than_becoming_a_seal_span() {
		// A guest answering 0 or an out-of-range span must yield no seal span; a wrapped value would hold the
		// frontier back on a schedule nobody chose.
		assert_eq!(scale_from_millis(Some(0)), None, "zero is not a seal span");
		assert_eq!(scale_from_millis(None), None);
		assert_eq!(
			scale_from_millis(Some(u64::MAX)),
			None,
			"a span past the representable range must not wrap into a short span"
		);
		assert_eq!(scale_from_millis(Some(65_000)), Some(ms(65_000)));
	}

	struct SealingInner {
		seal: Option<Duration>,
	}

	impl Operator<DeferredTransaction> for SealingInner {
		fn id(&self) -> OperatorId {
			OperatorId(7)
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			&[]
		}

		fn apply(&self, _txn: &mut DeferredTransaction, change: Change) -> Result<Change> {
			Ok(change)
		}

		fn seal_span(&self) -> Option<Duration> {
			self.seal
		}
	}

	#[test]
	fn a_declared_row_ttl_never_becomes_a_seal_span() {
		// A ttl says how long rows are kept, not how long a window stays amendable, so folding it in here would
		// hold every published frontier back by the whole retention window.
		let ttl_only = ApplyOperator::new(
			noop_parent(),
			OperatorId(7),
			Box::new(SealingInner {
				seal: None,
			}),
			Some(ms(3_600_000)),
		);
		assert_eq!(ttl_only.seal_span(), None);

		let sealing = ApplyOperator::new(
			noop_parent(),
			OperatorId(7),
			Box::new(SealingInner {
				seal: Some(ms(65_000)),
			}),
			Some(ms(3_600_000)),
		);
		assert_eq!(sealing.seal_span(), Some(ms(65_000)));
	}
}
