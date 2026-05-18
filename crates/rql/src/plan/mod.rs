// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Two-stage planner. `compile_logical` resolves names, type-checks, and produces a logical plan in terms of
//! `core::interface/` primitives; `compile_physical` lowers that into the executable shape the engine VM consumes.
//! `plan_with_policy` is the variant that interleaves a policy-injection pass between the two stages so read
//! filters land before the physical lowering.
//!
//! The split lets policy and optimisation operate on a backend-neutral representation; nothing in the logical layer
//! knows whether the table it reads from is a single-version or multi-version store.

use reifydb_catalog::catalog::Catalog;
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::{
	Result,
	ast::ast::AstStatement,
	bump::{Bump, BumpVec},
	expression::Expression,
	plan::{
		logical::{LogicalPlan, compile_logical},
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
) -> Result<Option<PhysicalPlan<'a>>> {
	let logical = compile_logical(bump, catalog, rx, statement)?;
	let physical = compile_physical(bump, catalog, rx, logical)?;
	Ok(physical)
}

#[instrument(name = "rql::plan_with_policy", level = "trace", skip(bump, catalog, rx, statement, policy))]
pub fn plan_with_policy<'a>(
	bump: &'a Bump,
	catalog: &Catalog,
	rx: &mut Transaction<'_>,
	statement: AstStatement<'a>,
	policy: impl Fn(
		BumpVec<'a, LogicalPlan<'a>>,
		&'a Bump,
		&Catalog,
		&mut Transaction<'_>,
	) -> Result<BumpVec<'a, LogicalPlan<'a>>>,
) -> Result<Option<PhysicalPlan<'a>>> {
	let logical = compile_logical(bump, catalog, rx, statement)?;
	let logical = policy(logical, bump, catalog, rx)?;
	let physical = compile_physical(bump, catalog, rx, logical)?;
	Ok(physical)
}
