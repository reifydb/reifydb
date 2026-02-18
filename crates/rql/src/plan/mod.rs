// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::{
	ast::ast::AstStatement,
	bump::Bump,
	expression::Expression,
	plan::{
		logical::compile_logical,
		physical::{PhysicalPlan, compile_physical},
	},
};

pub mod logical;
pub mod physical;

pub type RowToInsert = Vec<Expression>;

#[instrument(name = "rql::plan", level = "trace", skip(bump, catalog, rx, statement))]
pub fn plan<'a>(
	bump: &'a Bump,
	catalog: &Catalog,
	rx: &mut Transaction<'_>,
	statement: AstStatement<'a>,
) -> crate::Result<Option<PhysicalPlan<'a>>> {
	let logical = compile_logical(bump, catalog, rx, statement)?;
	let physical = compile_physical(bump, catalog, rx, logical)?;
	Ok(physical)
}
