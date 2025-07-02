// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstFilter;
use crate::plan::logical::{Compiler, FilterNode, LogicalPlan};

impl Compiler {
    pub(crate) fn compile_filter(ast: AstFilter) -> crate::Result<LogicalPlan> {
        Ok(LogicalPlan::Filter(FilterNode { condition: Self::compile_expression(*ast.node)? }))
    }
}
