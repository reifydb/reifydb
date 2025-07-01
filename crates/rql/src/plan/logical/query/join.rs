// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstJoin;
use crate::plan::expression;
use crate::plan::logical::{Compiler, JoinLeftNode, LogicalQueryPlan};

impl Compiler {
    pub(crate) fn compile_join(&self, ast: AstJoin) -> crate::Result<LogicalQueryPlan> {
        match ast {
            AstJoin::LeftJoin { with, on, .. } => Ok(LogicalQueryPlan::JoinLeft(JoinLeftNode {
                with: expression(*with)?,
                on: on.into_iter().map(expression).collect::<Result<Vec<_>, _>>()?,
            })),
        }
    }
}
