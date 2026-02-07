// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;

use crate::{
	plan::{
		logical,
		physical::{Compiler, CreateSubscriptionNode, PhysicalPlan},
	},
	query::QueryPlan,
};

impl Compiler {
	pub(crate) fn compile_create_subscription<T: AsTransaction>(
		&mut self,
		rx: &mut T,
		create: logical::CreateSubscriptionNode<'_>,
	) -> crate::Result<PhysicalPlan> {
		let as_clause = if !create.as_clause.is_empty() {
			// Compile logical plans to physical plan, then convert to QueryPlan
			let physical_plan = self.compile(rx, create.as_clause)?.unwrap();
			let query_plan: QueryPlan = physical_plan.try_into().expect("AS clause must be a query plan");
			Some(Box::new(query_plan))
		} else {
			None
		};

		Ok(PhysicalPlan::CreateSubscription(CreateSubscriptionNode {
			columns: create.columns,
			as_clause,
		}))
	}
}
