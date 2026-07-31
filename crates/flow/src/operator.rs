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

use crate::{timer::Timer, transaction::FlowTransaction};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reclaimable {
	pub data: Option<DateTime>,
	pub keyspaces: Vec<(Keyspace, DateTime)>,
	pub mapping: Option<DateTime>,
}

impl Reclaimable {
	pub fn data(at: DateTime) -> Self {
		Self {
			data: Some(at),
			..Self::default()
		}
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_none() && self.keyspaces.is_empty() && self.mapping.is_none()
	}
}

pub trait Operator: Send {
	fn id(&self) -> FlowNodeId;

	fn capabilities(&self) -> &[OperatorCapability];

	fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change>;

	fn on_timer(&self, _txn: &mut FlowTransaction, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}

	fn retention_scale(&self) -> Option<Duration> {
		None
	}

	fn reclaimable_through(&self, _txn: &mut FlowTransaction, _watermark: DateTime) -> Result<Reclaimable> {
		Ok(Reclaimable::default())
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn invalidate_groups(&self, _groups: &GroupSet) {}
}

pub type BoxedOperator = Box<dyn Operator + Send>;
