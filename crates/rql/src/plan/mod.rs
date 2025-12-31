// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

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

pub type RowToInsert = Vec<Expression>;

#[instrument(name = "rql::plan", level = "trace", skip(rx, statement))]
pub async fn plan<'a, T>(rx: &mut T, statement: AstStatement) -> crate::Result<Option<PhysicalPlan>>
where
	T: QueryTransaction + CatalogQueryTransaction,
{
	let logical = compile_logical(rx, statement).await?;
	let physical = compile_physical(rx, logical).await?;
	Ok(physical)
}
