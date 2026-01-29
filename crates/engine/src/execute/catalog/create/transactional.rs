// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::view::ViewToCreate;
use reifydb_core::{
	error::diagnostic::catalog::view_already_exists, interface::catalog::change::CatalogTrackViewChangeOperations,
	value::column::columns::Columns,
};
use reifydb_rql::plan::physical::CreateTransactionalViewNode;
use reifydb_transaction::transaction::command::CommandTransaction;
use reifydb_type::{return_error, value::Value};

use crate::execute::Executor;

impl Executor {
	pub(crate) fn create_transactional_view(
		&self,
		txn: &mut CommandTransaction,
		plan: CreateTransactionalViewNode,
	) -> crate::Result<Columns> {
		if let Some(view) = self.catalog.find_view_by_name(txn, plan.namespace.id, plan.view.text())? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.name.to_string())),
					("view", Value::Utf8(plan.view.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}

			return_error!(view_already_exists(plan.view.clone(), &plan.namespace.name, &view.name,));
		}

		let result = self.catalog.create_transactional_view(
			txn,
			ViewToCreate {
				fragment: Some(plan.view.clone()),
				name: plan.view.text().to_string(),
				namespace: plan.namespace.id,
				columns: plan.columns,
			},
		)?;
		txn.track_view_def_created(result.clone())?;

		self.create_deferred_view_flow(txn, &result, plan.as_clause)?;

		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name.to_string())),
			("view", Value::Utf8(plan.view.text().to_string())),
			("created", Value::Boolean(true)),
		]))
	}
}
