// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::plan::{
	logical,
	physical::{Compiler, CreateSubscriptionNode, PhysicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create_subscription(
		&self,
		create: logical::CreateSubscriptionNode,
	) -> crate::Result<PhysicalPlan> {
		Ok(PhysicalPlan::CreateSubscription(CreateSubscriptionNode {
			columns: create.columns,
		}))
	}
}
