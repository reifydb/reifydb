// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	evaluate::expression::Expression, UnderlyingQueryTransaction,
	VersionedQueryTransaction,
};

use crate::{
	ast::AstStatement,
	plan::{
		logical::compile_logical,
		physical::{compile_physical, PhysicalPlan},
	},
};

pub mod logical;
pub mod physical;

pub type RowToInsert = Vec<Expression>;

pub fn plan(
	rx: &mut impl UnderlyingQueryTransaction,
	statement: AstStatement,
) -> crate::Result<Option<PhysicalPlan>> {
	let logical = compile_logical(statement)?;
	let physical = compile_physical(rx, logical)?;
	Ok(physical)
}
