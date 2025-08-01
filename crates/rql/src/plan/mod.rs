// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::ast::AstStatement;
use crate::expression::Expression;

use crate::plan::logical::compile_logical;
use crate::plan::physical::{PhysicalPlan, compile_physical};
use reifydb_core::interface::VersionedReadTransaction;

pub mod logical;
pub mod physical;

pub type RowToInsert = Vec<Expression>;

pub fn plan(rx: &mut impl VersionedReadTransaction, statement: AstStatement) -> crate::Result<Option<PhysicalPlan>> {
    let logical = compile_logical(statement)?;
    let physical = compile_physical(rx, logical)?;
    Ok(physical)
}
