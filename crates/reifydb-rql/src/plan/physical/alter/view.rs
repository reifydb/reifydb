// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::interface::QueryTransaction;

use crate::plan::{
	logical::alter::{
		AlterViewNode, AlterViewOperation as LogicalAlterViewOp,
	},
	physical::{Compiler, PhysicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterViewPlan {
	pub node: AlterViewNode,
}

impl Compiler {
	pub(crate) fn compile_alter_view<T: QueryTransaction>(
		_rx: &mut T,
		alter: AlterViewNode,
	) -> crate::Result<PhysicalPlan> {
		// Convert logical plan to physical plan
		let plan = AlterViewPlan {
			node: alter,
		};
		Ok(PhysicalPlan::AlterView(plan))
	}
}
