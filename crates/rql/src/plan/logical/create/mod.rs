// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod schema;

use crate::ast::AstCreate;
use crate::plan::logical::{Compiler, LogicalPlan};

impl Compiler {
    pub(crate) fn compile_create(ast: AstCreate) -> crate::Result<LogicalPlan> {
        match ast{
            AstCreate::DeferredView { .. } => unimplemented!(),
            AstCreate::Schema { .. } => unimplemented!(),
            AstCreate::Series { .. } => unimplemented!(),
            AstCreate::Table { .. } => unimplemented!(),
        }
    }
}
