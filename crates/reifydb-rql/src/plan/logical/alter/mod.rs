// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::AstAlter,
	plan::logical::{Compiler, LogicalPlan},
};

mod sequence;
mod table;
mod view;

pub use table::{AlterTableNode, AlterTableOperation};
pub use view::{AlterViewNode, AlterViewOperation};

impl Compiler {
	pub(crate) fn compile_alter(
		ast: AstAlter,
	) -> crate::Result<LogicalPlan> {
		match ast {
			AstAlter::Sequence(node) => {
				Self::compile_alter_sequence(node)
			}
			AstAlter::Table(node) => {
				Self::compile_alter_table(node)
			}
			AstAlter::View(node) => Self::compile_alter_view(node),
		}
	}
}
