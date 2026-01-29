// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::AsTransaction;

use crate::{
	ast::ast::AstAlter,
	plan::logical::{Compiler, LogicalPlan},
};

pub mod flow;
pub mod sequence;
pub mod table;
pub mod view;

impl Compiler {
	pub(crate) fn compile_alter<T: AsTransaction>(&self, ast: AstAlter, tx: &mut T) -> crate::Result<LogicalPlan> {
		match ast {
			AstAlter::Sequence(node) => self.compile_alter_sequence(node),
			AstAlter::Table(node) => self.compile_alter_table(node),
			AstAlter::View(node) => self.compile_alter_view(node),
			AstAlter::Flow(node) => self.compile_alter_flow(node, tx),
		}
	}
}
