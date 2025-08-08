// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::AstTake;
use crate::plan::logical::{Compiler, LogicalPlan, TakeNode};

impl Compiler {
    pub(crate) fn compile_take(ast: AstTake) -> crate::Result<LogicalPlan> {
        Ok(LogicalPlan::Take(TakeNode { take: ast.take }))
    }
}
