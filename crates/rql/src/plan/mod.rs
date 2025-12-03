// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::interface::QueryTransaction;
use tracing::instrument;

use crate::{
	ast::AstStatement,
	expression::Expression,
	plan::{
		logical::compile_logical,
		physical::{PhysicalPlan, compile_physical},
	},
};

pub mod logical;
pub mod physical;

pub type RowToInsert = Vec<Expression<'static>>;

#[instrument(level = "trace", skip(rx, statement))]
pub fn plan<'a, T>(rx: &mut T, statement: AstStatement<'a>) -> crate::Result<Option<PhysicalPlan<'a>>>
where
	T: QueryTransaction + CatalogQueryTransaction,
{
	let logical = compile_logical(rx, statement)?;
	let physical = compile_physical(rx, logical)?;
	Ok(physical)
}
