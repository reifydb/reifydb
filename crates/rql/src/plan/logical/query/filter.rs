// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstFilter;
use crate::plan::logical::{Compiler, LogicalQueryPlan};

impl Compiler {
    pub(crate) fn compile_filter(&self, ast: AstFilter) -> crate::Result<Vec<LogicalQueryPlan>> {
        todo!()
    }
}
