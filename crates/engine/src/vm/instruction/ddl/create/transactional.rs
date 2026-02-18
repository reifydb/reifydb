// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::view::ViewToCreate;
use reifydb_core::{
	error::diagnostic::catalog::view_already_exists, interface::catalog::change::CatalogTrackViewChangeOperations,
	value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateTransactionalViewNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{return_error, value::Value};

use super::create_deferred_view_flow;
use crate::vm::services::Services;

pub(crate) fn create_transactional_view(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateTransactionalViewNode,
) -> crate::Result<Columns> {
	if let Some(view) =
		services.catalog.find_view_by_name(&mut Transaction::Admin(txn), plan.namespace.id, plan.view.text())?
	{
		if plan.if_not_exists {
			return Ok(Columns::single_row([
				("namespace", Value::Utf8(plan.namespace.name.to_string())),
				("view", Value::Utf8(plan.view.text().to_string())),
				("created", Value::Boolean(false)),
			]));
		}

		return_error!(view_already_exists(plan.view.clone(), &plan.namespace.name, &view.name,));
	}

	let result = services.catalog.create_transactional_view(
		txn,
		ViewToCreate {
			name: plan.view.clone(),
			namespace: plan.namespace.id,
			columns: plan.columns,
		},
	)?;
	txn.track_view_def_created(result.clone())?;

	create_deferred_view_flow(&services.catalog, txn, &result, *plan.as_clause)?;

	Ok(Columns::single_row([
		("namespace", Value::Utf8(plan.namespace.name.to_string())),
		("view", Value::Utf8(plan.view.text().to_string())),
		("created", Value::Boolean(true)),
	]))
}
