// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::QueryTransaction;

use crate::plan::{
	logical::alter::AlterViewNode,
	physical::{Compiler, PhysicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterViewPlan<'a> {
	pub node: AlterViewNode<'a>,
}

impl Compiler {
	pub(crate) fn compile_alter_view<'a>(
		_rx: &mut impl QueryTransaction,
		alter: AlterViewNode<'a>,
	) -> crate::Result<PhysicalPlan<'a>> {
		// Convert logical plan to physical plan
		let plan = AlterViewPlan {
			node: alter,
		};
		Ok(PhysicalPlan::AlterView(plan))
	}
}
