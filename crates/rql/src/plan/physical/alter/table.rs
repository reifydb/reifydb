// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::QueryTransaction;

use crate::plan::{
	logical,
	physical::{Compiler, PhysicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableNode {
	pub node: logical::alter::AlterTableNode,
}

impl Compiler {
	pub(crate) fn compile_alter_table(
		_rx: &mut impl QueryTransaction,
		alter: logical::alter::AlterTableNode,
	) -> crate::Result<PhysicalPlan> {
		// Convert logical plan to physical plan
		let plan = AlterTableNode {
			node: alter,
		};
		Ok(PhysicalPlan::AlterTable(plan))
	}
}
