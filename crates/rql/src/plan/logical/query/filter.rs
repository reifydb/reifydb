// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstFilter;
use crate::plan::expression;
use crate::plan::logical::{Compiler, FilterNode, LogicalQueryPlan};

impl Compiler {
    pub(crate) fn compile_filter(ast: AstFilter) -> crate::Result<LogicalQueryPlan> {
        Ok(LogicalQueryPlan::Filter(FilterNode { condition: expression(*ast.node)? }))
    }
}
