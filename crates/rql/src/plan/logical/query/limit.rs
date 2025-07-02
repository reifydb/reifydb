// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstLimit;
use crate::plan::logical::{Compiler, LimitNode, LogicalQueryPlan};

impl Compiler {
    pub(crate) fn compile_limit(ast: AstLimit) -> crate::Result<LogicalQueryPlan> {
        Ok(LogicalQueryPlan::Limit(LimitNode { limit: ast.limit }))
    }
}
