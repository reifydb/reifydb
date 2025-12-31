// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{CatalogStore, view::ViewToCreate};
use reifydb_core::{interface::CatalogTrackViewChangeOperations, return_error, value::column::Columns};
use reifydb_rql::plan::physical::CreateTransactionalViewNode;
use reifydb_type::{Value, diagnostic::catalog::view_already_exists};

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) async fn create_transactional_view(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: CreateTransactionalViewNode,
	) -> crate::Result<Columns> {
		if let Some(view) = self.catalog.find_view_by_name(txn, plan.namespace.id, plan.view.text()).await? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					("namespace", Value::Utf8(plan.namespace.name.to_string())),
					("view", Value::Utf8(plan.view.text().to_string())),
					("created", Value::Boolean(false)),
				]));
			}

			return_error!(view_already_exists(plan.view.clone(), &plan.namespace.name, &view.name,));
		}

		let result = CatalogStore::create_transactional_view(
			txn,
			ViewToCreate {
				fragment: Some(plan.view.clone()),
				name: plan.view.text().to_string(),
				namespace: plan.namespace.id,
				columns: plan.columns,
			},
		)
		.await?;
		txn.track_view_def_created(result.clone())?;

		self.create_deferred_view_flow(txn, &result, plan.as_clause).await?;

		Ok(Columns::single_row([
			("namespace", Value::Utf8(plan.namespace.name.to_string())),
			("view", Value::Utf8(plan.view.text().to_string())),
			("created", Value::Boolean(true)),
		]))
	}
}
