// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::QueryTransaction;

use crate::plan::{
	logical,
	physical::{Compiler, PhysicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterViewNode<'a> {
	pub node: logical::alter::AlterViewNode<'a>,
}

impl Compiler {
	pub(crate) fn compile_alter_view<'a>(
		_rx: &mut impl QueryTransaction,
		alter: logical::alter::AlterViewNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		// Convert logical plan to physical plan
		let plan = AlterViewNode {
			node: alter,
		};
		Ok(PhysicalPlan::AlterView(plan))
	}
}
