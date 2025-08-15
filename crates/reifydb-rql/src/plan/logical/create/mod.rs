// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod computed;
mod index;
mod schema;
mod series;
mod table;

use crate::{
	ast::AstCreate,
	plan::logical::{Compiler, LogicalPlan},
};

impl Compiler {
	pub(crate) fn compile_create(
		ast: AstCreate,
	) -> crate::Result<LogicalPlan> {
		match ast {
			AstCreate::ComputedView(node) => {
				Self::compile_computed_view(node)
			}
			AstCreate::Schema(node) => {
				Self::compile_create_schema(node)
			}
			AstCreate::Series(node) => {
				Self::compile_create_series(node)
			}
			AstCreate::Table(node) => {
				Self::compile_create_table(node)
			}
			AstCreate::Index(node) => {
				Self::compile_create_index(node)
			}
		}
	}
}
