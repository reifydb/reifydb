// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::AstFilter;
use crate::expression::ExpressionCompiler;
use crate::plan::logical::{Compiler, FilterNode, LogicalPlan};

impl Compiler {
    pub(crate) fn compile_filter(ast: AstFilter) -> crate::Result<LogicalPlan> {
        Ok(LogicalPlan::Filter(FilterNode { condition: ExpressionCompiler::compile(*ast.node)? }))
    }
}
