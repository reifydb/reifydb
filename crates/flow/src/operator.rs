// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::{catalog::flow::FlowNodeId, change::Change},
	key::operator_state::{GroupSet, Keyspace},
	metrics::heap::OperatorSample,
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};

use crate::transaction::{FlowTransaction, timer::Timer};

pub struct Tick {
	pub now: DateTime,
}

pub trait Operator: Send {
	fn id(&self) -> FlowNodeId;

	fn capabilities(&self) -> &[OperatorCapability];

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change>;

	fn on_timer(&self, _txn: &mut FlowTransaction, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}

	fn tick(&self, _txn: &mut FlowTransaction, _tick: Tick) -> Result<Option<Change>> {
		Ok(None)
	}

	fn ticks(&self) -> Option<Duration> {
		None
	}

	fn seal_after_ms(&self) -> Option<u64> {
		None
	}

	fn keyspace_spans(&self) -> Vec<(Keyspace, Duration)> {
		Vec::new()
	}

	fn sealed_through(&self, _txn: &mut FlowTransaction) -> Result<Option<DateTime>> {
		Ok(None)
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn invalidate_groups(&self, _groups: &GroupSet) {}
}

pub type BoxedOperator = Box<dyn Operator + Send>;
