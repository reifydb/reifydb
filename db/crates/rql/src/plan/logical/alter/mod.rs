// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstAlter,
	plan::logical::{Compiler, LogicalPlan, resolver::IdentifierResolver},
};

mod sequence;
mod table;
mod view;

pub use table::{
	AlterIndexColumn as AlterTableIndexColumn, AlterTableNode,
	AlterTableOperation,
};
pub use view::{
	AlterIndexColumn as AlterViewIndexColumn, AlterViewNode,
	AlterViewOperation,
};

impl Compiler {
	pub(crate) fn compile_alter<'a, 't, T: CatalogQueryTransaction>(
		ast: AstAlter<'a>,
		resolver: &mut IdentifierResolver<'t, T>,
	) -> crate::Result<LogicalPlan<'a>> {
		match ast {
			AstAlter::Sequence(node) => {
				Self::compile_alter_sequence(node, resolver)
			}
			AstAlter::Table(node) => {
				Self::compile_alter_table(node, resolver)
			}
			AstAlter::View(node) => {
				Self::compile_alter_view(node, resolver)
			}
		}
	}
}
