// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::lex::explain::explain_lex;
use crate::ast::parse::explain::explain_ast;
use crate::plan::logical::explain::explain_logical_plan;
use crate::plan::physical::explain::explain_physical_plan;
use reifydb_core::{Error, Explain};

pub struct ExplainAst {}

impl Explain for ExplainAst {
    fn explain(query: &str) -> Result<String, Error> {
        explain_ast(query)
    }
}

pub struct ExplainLex {}

impl Explain for ExplainLex {
    fn explain(query: &str) -> Result<String, Error> {
        explain_lex(query)
    }
}

pub struct ExplainLogicalPlan {}

impl Explain for ExplainLogicalPlan {
    fn explain(query: &str) -> Result<String, Error> {
        explain_logical_plan(query)
    }
}

pub struct ExplainPhysicalPlan {}

impl Explain for ExplainPhysicalPlan {
    fn explain(query: &str) -> Result<String, Error> {
        explain_physical_plan(query)
    }
}
