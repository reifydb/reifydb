// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use bumpalo::{Bump, collections::Vec as BumpVec};
use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::catalog::namespace::Namespace;
use reifydb_routine_abi::registry::Routines;
use reifydb_rql_frontend::ast::ast::AstStatement;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{error::ValueError, fragment::Fragment};
use tracing::instrument;

use crate::{
	error::RqlError,
	plan::{
		logical::{LogicalPlan, compile_logical},
		physical::{PhysicalPlan, compile_physical},
	},
};

pub mod logical;
pub mod physical;
pub mod system_columns;

#[instrument(name = "rql::plan", level = "trace", skip(bump, catalog, routines, rx, statement))]
pub fn plan<'a>(
	bump: &'a Bump,
	catalog: &Catalog,
	routines: &Routines,
	rx: &mut Transaction<'_>,
	statement: AstStatement<'a>,
) -> Result<Option<PhysicalPlan<'a>>, ValueError> {
	let logical = compile_logical(bump, catalog, rx, statement)?;
	let physical = compile_physical(bump, catalog, routines, rx, logical)?;
	Ok(physical)
}

#[instrument(name = "rql::plan_with_policy", level = "trace", skip(bump, catalog, routines, rx, statement, policy))]
pub fn plan_with_policy<'a>(
	bump: &'a Bump,
	catalog: &Catalog,
	routines: &Routines,
	rx: &mut Transaction<'_>,
	statement: AstStatement<'a>,
	policy: impl Fn(
		BumpVec<'a, LogicalPlan<'a>>,
		&'a Bump,
		&Catalog,
		&mut Transaction<'_>,
	) -> Result<BumpVec<'a, LogicalPlan<'a>>, ValueError>,
) -> Result<Option<PhysicalPlan<'a>>, ValueError> {
	let logical = compile_logical(bump, catalog, rx, statement)?;
	let logical = policy(logical, bump, catalog, rx)?;
	let physical = compile_physical(bump, catalog, routines, rx, logical)?;
	Ok(physical)
}

pub(crate) fn view_not_writable(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	namespace: &Namespace,
	name: &str,
	operation: &'static str,
	fragment: Fragment,
) -> Result<Option<ValueError>, ValueError> {
	if namespace.address().is_some() || catalog.find_view_by_name(tx, namespace.id(), name)?.is_none() {
		return Ok(None);
	}
	Ok(Some(RqlError::ViewNotWritable {
		namespace: namespace.name().to_string(),
		view: name.to_string(),
		operation,
		fragment,
	}
	.into()))
}
