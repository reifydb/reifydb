// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::Error;
use reifydb::rql::explain::{
    explain_ast, explain_lex, explain_logical_plan, explain_physical_plan,
};
use reifydb::transaction::test_utils::create_test_write_transaction;

pub fn lex(query: &str) -> Result<(), Error> {
    let text = explain_lex(query)?;
    println!("{}", text);
    Ok(())
}

pub fn ast(query: &str) -> Result<(), Error> {
    let text = explain_ast(query)?;
    println!("{}", text);
    Ok(())
}

pub fn logical_plan(query: &str) -> Result<(), Error> {
    let text = explain_logical_plan(query)?;
    println!("{}", text);
    Ok(())
}

pub fn physical_plan(query: &str) -> Result<(), Error> {
    let mut dummy_tx = create_test_write_transaction();
    let text = explain_physical_plan(&mut dummy_tx, query)?;
    println!("{}", text);
    Ok(())
}
