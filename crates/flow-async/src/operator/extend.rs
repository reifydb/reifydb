// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	value::column::columns::Columns,
};
use reifydb_flow::operator::extend::ExtendOperator;
use reifydb_value::Result;

use crate::operator::{HostOperator, host::HostContext};

impl HostOperator for ExtendOperator {
	fn id(&self) -> OperatorId {
		self.id()
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
		self.apply(change)
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}
