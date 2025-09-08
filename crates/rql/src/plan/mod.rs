// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

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

pub type RowToInsert = Vec<Expression<'static>>;

pub fn plan<'a>(
	rx: &mut impl QueryTransaction,
	statement: AstStatement<'a>,
) -> crate::Result<Option<PhysicalPlan<'a>>> {
	let logical = compile_logical(statement)?;
	let physical = compile_physical(rx, logical)?;
	Ok(physical)
}

pub fn logical_all<'a>(
	statements: Vec<AstStatement<'a>>,
) -> crate::Result<Vec<LogicalPlan<'a>>> {
	let mut result = vec![];

	for statement in statements {
		result.extend(compile_logical(statement)?);
	}

	Ok(result)
}
