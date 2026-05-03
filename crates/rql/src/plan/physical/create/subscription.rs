// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::{
	Result,
	plan::{
		logical,
		physical::{Compiler, CreateSubscriptionNode, PhysicalPlan},
	},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_subscription(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateSubscriptionNode<'bump>,
	) -> Result<PhysicalPlan<'bump>> {
		let as_clause = if !create.as_clause.is_empty() {
			let physical_plan = self.compile(rx, create.as_clause)?.unwrap();
			Some(self.bump_box(physical_plan))
		} else {
			None
		};

		Ok(PhysicalPlan::CreateSubscription(CreateSubscriptionNode {
			columns: create.columns,
			as_clause,
		}))
	}
}
