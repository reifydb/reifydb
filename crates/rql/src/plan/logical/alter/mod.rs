// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::IntoStandardTransaction;

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
	pub(crate) fn compile_alter<T: IntoStandardTransaction>(
		&self,
		ast: AstAlter,
		tx: &mut T,
	) -> crate::Result<LogicalPlan> {
		match ast {
			AstAlter::Sequence(node) => self.compile_alter_sequence(node),
			AstAlter::Table(node) => self.compile_alter_table(node),
			AstAlter::View(node) => self.compile_alter_view(node),
			AstAlter::Flow(node) => self.compile_alter_flow(node, tx),
		}
	}
}
