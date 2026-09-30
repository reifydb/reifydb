// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_schema::SchemaRef;
use reifydb_core::interface::{catalog::flow::OperatorId, change::Change};

pub struct SortOperator {
	operator: OperatorId,
	parent_schema: Option<SchemaRef>,
}

impl SortOperator {
	pub fn new(parent_schema: Option<SchemaRef>, operator: OperatorId) -> Self {
		Self {
			operator,
			parent_schema,
		}
	}

	pub fn id(&self) -> OperatorId {
		self.operator
	}

	pub fn output_schema(&self) -> Option<SchemaRef> {
		self.parent_schema.clone()
	}

	pub fn apply(&self, change: Change) -> Change {
		Change::from_flow(self.operator, change.version, change.diffs, change.changed_at)
	}
}
