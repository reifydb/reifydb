// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogStore, view::ViewToCreate};
use reifydb_core::{
	Value, interface::Transaction,
	result::error::diagnostic::catalog::view_already_exists, return_error,
};
use reifydb_rql::plan::physical::CreateTransactionalViewPlan;

use crate::{StandardCommandTransaction, columnar::Columns, execute::Executor};

impl Executor {
	pub(crate) fn create_transactional_view<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: CreateTransactionalViewPlan,
	) -> crate::Result<Columns> {
		if let Some(view) = CatalogStore::find_view_by_name(
			txn,
			plan.schema.id,
			&plan.view,
		)? {
			if plan.if_not_exists {
				return Ok(Columns::single_row([
					(
						"schema",
						Value::Utf8(
							plan.schema
								.name
								.to_string(),
						),
					),
					(
						"view",
						Value::Utf8(
							plan.view.to_string(),
						),
					),
					("created", Value::Bool(false)),
				]));
			}

			return_error!(view_already_exists(
				Some(plan.view.clone()),
				&plan.schema.name,
				&view.name,
			));
		}

		let result = CatalogStore::create_transactional_view(
			txn,
			ViewToCreate {
				fragment: Some(plan.view.clone()),
				view: plan.view.to_string(),
				schema: plan.schema.name.to_string(),
				columns: plan.columns,
			},
		)?;

		self.create_flow(txn, &result, plan.with)?;

		Ok(Columns::single_row([
			("schema", Value::Utf8(plan.schema.name.to_string())),
			("view", Value::Utf8(plan.view.to_string())),
			("created", Value::Bool(true)),
		]))
	}
}
