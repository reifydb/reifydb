// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::rql::{ExplainAst, ExplainLex, ExplainLogicalPlan, ExplainPhysicalPlan};
use reifydb::{Error, Explain};

pub fn lex(query: &str) -> Result<(), Error> {
    let text = ExplainLex::explain(query)?;
    println!("{}", text);
    Ok(())
}

pub fn ast(query: &str) -> Result<(), Error> {
    let text = ExplainAst::explain(query)?;
    println!("{}", text);
    Ok(())
}

pub fn logical_plan(query: &str) -> Result<(), Error> {
    let text = ExplainLogicalPlan::explain(query)?;
    println!("{}", text);
    Ok(())
}

pub fn physical_plan(query: &str) -> Result<(), Error> {
    let text = ExplainPhysicalPlan::explain(query)?;
    println!("{}", text);
    Ok(())
}
