// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::AstMap;
use crate::plan::logical::{Compiler, LogicalPlan, MapNode};

impl Compiler {
    pub(crate) fn compile_map(ast: AstMap) -> crate::Result<LogicalPlan> {
        Ok(LogicalPlan::Map(MapNode {
            map: ast
                .nodes
                .into_iter()
                .map(Self::compile_expression)
                .collect::<crate::Result<Vec<_>>>()?,
        }))
    }
}
