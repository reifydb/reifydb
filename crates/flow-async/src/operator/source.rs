// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_schema::SchemaRef;
use reifydb_core::interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability};
use reifydb_flow::operator::source::SourceOperator;
use reifydb_value::Result;

use crate::operator::{HostOperator, host::HostContext};

impl HostOperator for SourceOperator {
	fn id(&self) -> OperatorId {
		self.id()
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		OperatorCapability::STANDARD
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		SourceOperator::apply(self, host, change)
	}

	fn output_schema(&self) -> Option<SchemaRef> {
		self.output_schema()
	}
}
