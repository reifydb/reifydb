// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::Transaction;

use crate::plan::{
	logical,
	physical::{Compiler, CreateSubscriptionNode, PhysicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_subscription(
		&mut self,
		rx: &mut Transaction<'_>,
		create: logical::CreateSubscriptionNode<'bump>,
	) -> crate::Result<PhysicalPlan<'bump>> {
		let as_clause = if !create.as_clause.is_empty() {
			// Compile logical plans to physical plan
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
