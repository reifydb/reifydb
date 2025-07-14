// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod deferred_view;
mod schema;
mod series;
mod table;

use crate::ast::AstCreate;
use crate::plan::logical::{Compiler, LogicalPlan};

impl Compiler {
    pub(crate) fn compile_create(ast: AstCreate) -> crate::Result<LogicalPlan> {
        match ast {
            AstCreate::DeferredView(node) => Self::compile_deferred_view(node),
            AstCreate::Schema(node) => Self::compile_create_schema(node),
            AstCreate::Series(node) => Self::compile_create_series(node),
            AstCreate::Table(node) => Self::compile_create_table(node),
        }
    }
}
