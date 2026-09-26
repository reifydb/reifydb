// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_flow::operator::append::AppendOperator;
use reifydb_value::Result;

use crate::{
	operator::{HostOperator, host::HostContext},
	timer::Timer,
};

const CAPABILITIES: &[OperatorCapability] = OperatorCapability::STANDARD;

impl HostOperator for AppendOperator {
	fn id(&self) -> OperatorId {
		self.id()
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		CAPABILITIES
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
		self.apply(change)
	}

	fn on_timer(&mut self, _host: &mut dyn HostContext, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_flow::operator::append::lane::AppendLanes;

	use super::*;

	#[test]
	fn append_reports_no_operator_sample() {
		assert!(HostOperator::sample(&AppendOperator::new(
			OperatorId(1),
			None,
			vec![OperatorId(2), OperatorId(3)],
			AppendLanes::new(OperatorId(1), 1, [Some(0), Some(1)]),
		))
		.is_none());
	}
}
