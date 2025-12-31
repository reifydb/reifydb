// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::Catalog;
use reifydb_transaction::IntoStandardTransaction;
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

pub type RowToInsert = Vec<Expression>;

#[instrument(name = "rql::plan", level = "trace", skip(catalog, rx, statement))]
pub async fn plan<T: IntoStandardTransaction>(
	catalog: &Catalog,
	rx: &mut T,
	statement: AstStatement,
) -> crate::Result<Option<PhysicalPlan>> {
	let logical = compile_logical(catalog, rx, statement).await?;
	let physical = compile_physical(catalog, rx, logical).await?;
	Ok(physical)
}
