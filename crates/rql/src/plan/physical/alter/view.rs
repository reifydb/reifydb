// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::IntoStandardTransaction;

use crate::plan::{
	logical,
	physical::{Compiler, PhysicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterViewNode {
	pub node: logical::alter::AlterViewNode,
}

impl Compiler {
	pub(crate) fn compile_alter_view<T: IntoStandardTransaction>(
		&self,
		_rx: &mut T,
		alter: logical::alter::AlterViewNode,
	) -> crate::Result<PhysicalPlan> {
		// Convert logical plan to physical plan
		let plan = AlterViewNode {
			node: alter,
		};
		Ok(PhysicalPlan::AlterView(plan))
	}
}
