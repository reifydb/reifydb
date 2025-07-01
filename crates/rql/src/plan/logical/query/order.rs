// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstOrder;
use crate::plan::logical::{Compiler, LogicalQueryPlan};

impl Compiler {
    pub(crate) fn compile_order(&self, ast: AstOrder) -> crate::Result<Vec<LogicalQueryPlan>> {
        todo!()
    }
}
