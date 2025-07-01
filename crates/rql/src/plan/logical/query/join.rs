// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstJoin;
use crate::plan::logical::{Compiler, LogicalQueryPlan};

impl Compiler {
    pub(crate) fn compile_join(&self, ast: AstJoin) -> crate::Result<Vec<LogicalQueryPlan>> {
        todo!()
    }
}
