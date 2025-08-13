// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	ast::AstAlter,
	plan::logical::{Compiler, LogicalPlan},
};

mod sequence;

impl Compiler {
	pub(crate) fn compile_alter(
		ast: AstAlter,
	) -> crate::Result<LogicalPlan> {
		match ast {
			AstAlter::Sequence(node) => {
				Self::compile_alter_sequence(node)
			}
		}
	}
}
