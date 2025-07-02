// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstSelect;
use crate::plan::logical::{Compiler, LogicalQueryPlan, SelectNode};

impl Compiler {
    pub(crate) fn compile_select(ast: AstSelect) -> crate::Result<LogicalQueryPlan> {
        Ok(LogicalQueryPlan::Select(SelectNode {
            select: ast
                .select
                .into_iter()
                .map(Self::compile_expression)
                .collect::<Result<Vec<_>, _>>()?,
        }))
    }
}
