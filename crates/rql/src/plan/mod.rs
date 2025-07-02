// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::ast::AstStatement;
use crate::expression::Expression;

use crate::plan::logical::{compile_logical, compile_logical_query};
use crate::plan::physical::{
    PhysicalPlan, PhysicalQueryPlan, compile_physical, compile_physical_query,
};
use reifydb_core::Error;
use reifydb_core::interface::{Rx, UnversionedStorage, VersionedStorage};

pub mod logical;
pub mod physical;

pub type RowToInsert = Vec<Expression>;

pub type Result<T> = std::result::Result<T, Error>;

pub fn plan<VS: VersionedStorage, US: UnversionedStorage>(
    statement: AstStatement,
) -> Result<Option<PhysicalPlan>> {
    let logical = compile_logical(statement)?;
    let physical = compile_physical(logical)?;
    Ok(physical)
}

pub fn plan_query(ast: AstStatement) -> Result<Option<PhysicalQueryPlan>> {
    let logical = compile_logical_query(ast)?;
    let physical = compile_physical_query(logical)?;
    Ok(physical)
}
