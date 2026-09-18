// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	metrics::heap::OperatorSample,
	operator_with::{ApplyWith, WithSpan},
	value::column::columns::Columns,
};
use reifydb_value::{Result, value::duration::Duration};

use crate::{
	operator::{
		BoxedHostOperator, HostOperator, host::HostContext, max_input_time, stamp_output_time,
		state::seal::rule::SealRule,
	},
	timer::Timer,
};

pub struct ApplyOperator {
	parent_schema: Option<Columns>,
	operator: OperatorId,
	inner: BoxedHostOperator,
	seal_span: Option<Duration>,
}

impl ApplyOperator {
	pub fn new(
		parent_schema: Option<Columns>,
		operator: OperatorId,
		inner: BoxedHostOperator,
		with: &ApplyWith,
	) -> Self {
		Self {
			parent_schema,
			operator,
			inner,
			seal_span: engine_seal_span(with),
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

	fn seal_span(&self) -> Option<Duration> {
		self.seal_span
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

fn engine_seal_span(with: &ApplyWith) -> Option<Duration> {
	let span = match &with.window {
		Some(kind) => {
			let lateness = match with.lateness {
				Some(WithSpan::Duration(d)) => d,
				_ => Duration::zero(),
			};
			SealRule::for_window(kind, lateness).map(|rule| rule.admissible().duration())
		}
		None => match with.lateness {
			Some(WithSpan::Duration(d)) => Some(d),
			_ => None,
		},
	};
	span.filter(|span| !span.is_zero())
}

#[cfg(test)]
mod tests {
	use reifydb_core::common::{WindowKind, WindowSize};

	use super::*;

	struct Stub;

	impl HostOperator for Stub {
		fn id(&self) -> OperatorId {
			OperatorId(1)
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
			Ok(change)
		}
	}

	fn seconds(seconds: i64) -> Duration {
		Duration::from_milliseconds_const(seconds * 1_000)
	}

	fn operator(with: ApplyWith) -> ApplyOperator {
		ApplyOperator::new(None, OperatorId(1), Box::new(Stub), &with)
	}

	#[test]
	fn a_windowed_apply_holds_by_size_plus_lateness() {
		// The engine span for a windowed apply must come from SealRule::for_window, not the guest.
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(seconds(60)),
			}),
			lateness: Some(WithSpan::Duration(seconds(30))),
			immutable: None,
		};

		assert_eq!(operator(with).seal_span(), Some(seconds(90)));
	}

	#[test]
	fn an_apply_without_a_window_holds_by_its_lateness() {
		// An apply with no window still seals by its own lateness, otherwise a bare apply never holds.
		let with = ApplyWith {
			window: None,
			lateness: Some(WithSpan::Duration(seconds(30))),
			immutable: None,
		};

		assert_eq!(operator(with).seal_span(), Some(seconds(30)));
	}

	#[test]
	fn an_apply_without_a_window_or_lateness_holds_nothing() {
		// Nothing in `with` gives the engine a span, so the watermark must pass through untouched.
		let with = ApplyWith::default();

		assert_eq!(operator(with).seal_span(), None);
	}

	#[test]
	fn a_slot_window_holds_nothing_in_the_engine() {
		// A count-sized window seals in a row coordinate the engine has no duration for.
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(10),
			}),
			lateness: Some(WithSpan::Count(2)),
			immutable: None,
		};

		assert_eq!(operator(with).seal_span(), None);
	}

	#[test]
	fn a_rolling_apply_holds_by_the_rolling_rule() {
		// Rolling must go through the rolling arm of for_window, which adds lag to size before lateness.
		let with = ApplyWith {
			window: Some(WindowKind::Rolling {
				size: WindowSize::Duration(seconds(120)),
				lag: Some(seconds(10)),
			}),
			lateness: Some(WithSpan::Duration(seconds(5))),
			immutable: None,
		};

		assert_eq!(operator(with).seal_span(), Some(seconds(135)));
	}

	#[test]
	fn a_zero_span_reports_none() {
		// A zero span must read as no hold, matching what the guest mount reports today.
		let with = ApplyWith {
			window: None,
			lateness: Some(WithSpan::Duration(Duration::zero())),
			immutable: None,
		};

		assert_eq!(operator(with).seal_span(), None);
	}
}
