// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::interface::QueryTransaction;

use crate::plan::{
	logical::alter::{
		AlterTableNode, AlterTableOperation as LogicalAlterTableOp,
	},
	physical::{Compiler, PhysicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTablePlan<'a> {
	pub node: AlterTableNode<'a>,
}

impl Compiler {
	pub(crate) fn compile_alter_table<'a>(
		_rx: &mut impl QueryTransaction,
		alter: AlterTableNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		// Convert logical plan to physical plan
		let plan = AlterTablePlan {
			node: alter,
		};
		Ok(PhysicalPlan::AlterTable(plan))
	}
}
