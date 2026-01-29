// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;

use crate::plan::{
	logical,
	physical::{Compiler, PhysicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterViewNode {
	pub node: logical::alter::view::AlterViewNode,
}

impl Compiler {
	pub(crate) fn compile_alter_view<T: AsTransaction>(
		&self,
		_rx: &mut T,
		alter: logical::alter::view::AlterViewNode,
	) -> crate::Result<PhysicalPlan> {
		// Convert logical plan to physical plan
		let plan = AlterViewNode {
			node: alter,
		};
		Ok(PhysicalPlan::AlterView(plan))
	}
}
