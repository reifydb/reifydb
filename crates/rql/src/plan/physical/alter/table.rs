// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;

use crate::plan::{
	logical,
	physical::{Compiler, PhysicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableNode {
	pub node: logical::alter::table::AlterTableNode,
}

impl Compiler {
	pub(crate) fn compile_alter_table<T: AsTransaction>(
		&self,
		_rx: &mut T,
		alter: logical::alter::table::AlterTableNode,
	) -> crate::Result<PhysicalPlan> {
		// Convert logical plan to physical plan
		let plan = AlterTableNode {
			node: alter,
		};
		Ok(PhysicalPlan::AlterTable(plan))
	}
}
