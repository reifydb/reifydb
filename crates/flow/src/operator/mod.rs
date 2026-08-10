// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change},
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_value::{Result, value::duration::Duration};

use crate::{timer::Timer, transaction::FlowTransaction};

pub trait Operator: Send {
	fn id(&self) -> OperatorId;

	fn capabilities(&self) -> &[OperatorCapability];

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change>;

	fn on_timer(&self, _txn: &mut FlowTransaction, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}

	fn seal_span(&self) -> Option<Duration> {
		None
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn output_schema(&self) -> Option<Columns> {
		None
	}
}

pub type BoxedOperator = Box<dyn Operator + Send>;
