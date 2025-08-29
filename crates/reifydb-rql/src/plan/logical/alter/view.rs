// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::{AstAlterView, AstAlterViewOperation},
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterViewNode<'a> {
	pub view: AstAlterView<'a>, // TODO: Fix lifetime
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterViewOperation {
	CreatePrimaryKey,
	DropPrimaryKey,
}

impl Compiler {
	pub(crate) fn compile_alter_view(
		ast: AstAlterView,
	) -> crate::Result<LogicalPlan> {
		// Convert the AST to a logical plan node
		let node = AlterViewNode {
			view: unsafe { std::mem::transmute(ast) }, /* TODO: Fix lifetime properly */
		};
		Ok(LogicalPlan::AlterView(node))
	}
}
