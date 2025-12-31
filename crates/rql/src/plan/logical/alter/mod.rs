// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::CatalogQueryTransaction;

use crate::{
	ast::AstAlter,
	plan::logical::{Compiler, LogicalPlan},
};

mod flow;
mod sequence;
mod table;
mod view;

pub use flow::{AlterFlowAction, AlterFlowNode};
pub use table::{AlterIndexColumn as AlterTableIndexColumn, AlterTableNode, AlterTableOperation};
pub use view::{AlterIndexColumn as AlterViewIndexColumn, AlterViewNode, AlterViewOperation};

impl Compiler {
	pub(crate) async fn compile_alter<T: CatalogQueryTransaction>(
		ast: AstAlter,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		match ast {
			AstAlter::Sequence(node) => Self::compile_alter_sequence(node, tx),
			AstAlter::Table(node) => Self::compile_alter_table(node, tx),
			AstAlter::View(node) => Self::compile_alter_view(node, tx),
			AstAlter::Flow(node) => Self::compile_alter_flow(node, tx).await,
		}
	}
}
