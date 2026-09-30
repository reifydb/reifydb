// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_schema::SchemaRef;
use reifydb_core::interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability};
use reifydb_flow::operator::sort::SortOperator;
use reifydb_value::Result;

use crate::operator::{HostOperator, host::HostContext};

impl HostOperator for SortOperator {
	fn id(&self) -> OperatorId {
		self.id()
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
		Ok(SortOperator::apply(self, change))
	}

	fn output_schema(&self) -> Option<SchemaRef> {
		self.output_schema()
	}
}
