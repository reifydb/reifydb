// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::{catalog::flow::FlowNodeId, change::Change},
	key::operator_state::GroupSet,
	metrics::heap::OperatorSample,
};
use reifydb_sdk::operator::Tick;
use reifydb_value::{Result, value::duration::Duration};

use crate::transaction::FlowTransaction;

pub trait Operator: Send {
	fn id(&self) -> FlowNodeId;

	fn capabilities(&self) -> &[OperatorCapability];

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change>;

	fn tick(&self, _txn: &mut FlowTransaction, _tick: Tick) -> Result<Option<Change>> {
		Ok(None)
	}

	fn ticks(&self) -> Option<Duration> {
		None
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn invalidate_groups(&self, _groups: &GroupSet) {}
}

pub type BoxedOperator = Box<dyn Operator + Send>;
