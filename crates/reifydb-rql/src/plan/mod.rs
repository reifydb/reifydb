// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::interface::{
	QueryTransaction, evaluate::expression::Expression,
};

use crate::{
	ast::AstStatement,
	plan::{
		logical::{LogicalPlan, compile_logical},
		physical::{PhysicalPlan, compile_physical},
	},
};

pub mod logical;
pub mod physical;

pub type RowToInsert = Vec<Expression>;

pub fn plan<T>(
	rx: &mut T,
	statement: AstStatement,
) -> crate::Result<Option<PhysicalPlan>>
where
	T: QueryTransaction + CatalogQueryTransaction,
{
	let logical = compile_logical(statement)?;
	let physical = compile_physical(rx, logical)?;
	Ok(physical)
}

pub fn logical_all(
	statements: Vec<AstStatement>,
) -> crate::Result<Vec<LogicalPlan>> {
	let mut result = vec![];

	for statement in statements {
		result.extend(compile_logical(statement)?);
	}

	Ok(result)
}
