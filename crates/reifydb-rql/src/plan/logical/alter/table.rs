// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::{AstAlterTable, AstAlterTableOperation},
	plan::logical::{Compiler, LogicalPlan},
};

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableNode {
	pub table: AstAlterTable<'static>, // TODO: Fix lifetime
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOperation {
	CreatePrimaryKey,
	DropPrimaryKey,
}

impl Compiler {
	pub(crate) fn compile_alter_table(
		ast: AstAlterTable,
	) -> crate::Result<LogicalPlan> {
		// Convert the AST to a logical plan node
		let node = AlterTableNode {
			table: unsafe { std::mem::transmute(ast) }, /* TODO: Fix lifetime properly */
		};
		Ok(LogicalPlan::AlterTable(node))
	}
}
