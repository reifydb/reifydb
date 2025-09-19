// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::QueryTransaction;

use crate::plan::{
	logical,
	physical::{Compiler, PhysicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableNode<'a> {
	pub node: logical::alter::AlterTableNode<'a>,
}

impl Compiler {
	pub(crate) fn compile_alter_table<'a>(
		_rx: &mut impl QueryTransaction,
		alter: logical::alter::AlterTableNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		// Convert logical plan to physical plan
		let plan = AlterTableNode {
			node: alter,
		};
		Ok(PhysicalPlan::AlterTable(plan))
	}
}
