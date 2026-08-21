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
}

impl ApplyOperator {
	pub fn new(parent_schema: Option<Columns>, operator: OperatorId, inner: BoxedHostOperator) -> Self {
		Self {
			parent_schema,
			operator,
			inner,
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
		self.inner.seal_span()
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
