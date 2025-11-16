// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstAlter,
	plan::logical::{Compiler, LogicalPlan},
};

mod sequence;
mod table;
mod view;

pub use table::{AlterIndexColumn as AlterTableIndexColumn, AlterTableNode, AlterTableOperation};
pub use view::{AlterIndexColumn as AlterViewIndexColumn, AlterViewNode, AlterViewOperation};

impl Compiler {
	pub(crate) fn compile_alter<'a, T: CatalogQueryTransaction>(
		ast: AstAlter<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		match ast {
			AstAlter::Sequence(node) => Self::compile_alter_sequence(node, tx),
			AstAlter::Table(node) => Self::compile_alter_table(node, tx),
			AstAlter::View(node) => Self::compile_alter_view(node, tx),
			AstAlter::Flow(_) => {
				// TODO: Implement ALTER FLOW compilation
				todo!("ALTER FLOW compilation not yet implemented")
			}
		}
	}
}
