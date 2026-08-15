// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_value::{Result, value::duration::Duration};

use crate::{
	operator::{BoxedHostOperator, HostOperator, host::HostContext, max_input_time, stamp_output_time},
	timer::Timer,
};

pub struct ApplyOperator {
	parent_schema: Option<Columns>,
	operator: OperatorId,
	inner: BoxedHostOperator,
	_lateness: Option<Duration>,
}

impl ApplyOperator {
	pub fn new(
		parent_schema: Option<Columns>,
		operator: OperatorId,
		inner: BoxedHostOperator,
		lateness: Option<Duration>,
	) -> Self {
		Self {
			parent_schema,
			operator,
			inner,
			_lateness: lateness,
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent_schema.clone()
	}
}

impl HostOperator for ApplyOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		self.inner.capabilities()
	}

	fn lateness_span(&self) -> Option<Duration> {
		self.inner.lateness_span()
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		let inherited = max_input_time(&change);
		let mut out = self.inner.apply(host, change)?;
		stamp_output_time(&mut out, inherited);
		Ok(out)
	}

	fn on_timer(&mut self, host: &mut dyn HostContext, timer: Timer) -> Result<Option<Change>> {
		let due = timer.due;
		let mut out = self.inner.on_timer(host, timer)?;
		if let Some(change) = out.as_mut() {
			stamp_output_time(change, Some(due));
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
	use reifydb_core::interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability};
	use reifydb_value::{Result, value::duration::Duration};

	use super::ApplyOperator;
	use crate::operator::{HostOperator, host::HostContext, scale_from_millis};

	fn ms(milliseconds: i64) -> Duration {
		Duration::from_milliseconds(milliseconds).expect("representable duration")
	}

	#[test]
	fn an_unusable_guest_span_is_refused_rather_than_becoming_a_lateness_span() {
		// A guest answering 0 or an out-of-range span must yield no lateness span; a wrapped value would hold
		// the frontier back on a schedule nobody chose.
		assert_eq!(scale_from_millis(Some(0)), None, "zero is not a lateness span");
		assert_eq!(scale_from_millis(None), None);
		assert_eq!(
			scale_from_millis(Some(u64::MAX)),
			None,
			"a span past the representable range must not wrap into a short span"
		);
		assert_eq!(scale_from_millis(Some(65_000)), Some(ms(65_000)));
	}

	struct SealingInner {
		lateness: Option<Duration>,
	}

	impl HostOperator for SealingInner {
		fn id(&self) -> OperatorId {
			OperatorId(7)
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			&[]
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
			Ok(change)
		}

		fn lateness_span(&self) -> Option<Duration> {
			self.lateness
		}
	}

	#[test]
	fn a_declared_row_ttl_never_becomes_a_lateness_span() {
		// A ttl says how long rows are kept, not how long a window stays amendable, so folding it in here would
		// hold every published frontier back by the whole retention window.
		let ttl_only = ApplyOperator::new(
			None,
			OperatorId(7),
			Box::new(SealingInner {
				lateness: None,
			}),
			Some(ms(3_600_000)),
		);
		assert_eq!(ttl_only.lateness_span(), None);

		let sealing = ApplyOperator::new(
			None,
			OperatorId(7),
			Box::new(SealingInner {
				lateness: Some(ms(65_000)),
			}),
			Some(ms(3_600_000)),
		);
		assert_eq!(sealing.lateness_span(), Some(ms(65_000)));
	}
}
