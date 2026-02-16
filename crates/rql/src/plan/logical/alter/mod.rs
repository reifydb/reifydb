// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;

use crate::{
	ast::ast::AstAlter,
	plan::logical::{Compiler, LogicalPlan},
};

pub mod flow;
pub mod reducer;
pub mod sequence;
pub mod table;
pub mod view;

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_alter<T: AsTransaction>(
		&self,
		ast: AstAlter<'bump>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'bump>> {
		match ast {
			AstAlter::Sequence(node) => self.compile_alter_sequence(node),
			AstAlter::Table(node) => self.compile_alter_table(node),
			AstAlter::View(node) => self.compile_alter_view(node),
			AstAlter::Flow(node) => self.compile_alter_flow(node, tx),
			AstAlter::Reducer(node) => self.compile_alter_reducer(node, tx),
		}
	}
}
