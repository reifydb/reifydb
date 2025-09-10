// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogStore, view::ViewToCreate};
use reifydb_core::{
	interface::Transaction, return_error, value::columnar::Columns,
};
use reifydb_rql::plan::physical::CreateTransactionalViewPlan;
use reifydb_type::{Value, diagnostic::catalog::view_already_exists};

use crate::{StandardCommandTransaction, execute::Executor};

impl Executor {
	pub(crate) fn create_transactional_view<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: CreateTransactionalViewPlan,
	) -> crate::Result<Columns> {
		if let Some(view) = CatalogStore::find_view_by_name(
			txn,
			plan.schema.id,
			plan.view.name.text(),
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
							plan.view
								.name
								.text()
								.to_string(),
						),
					),
					("created", Value::Boolean(false)),
				]));
			}

			return_error!(view_already_exists(
				Some(plan.view.name.clone().into_owned()),
				&plan.schema.name,
				&view.name,
			));
		}

		let result = CatalogStore::create_transactional_view(
			txn,
			ViewToCreate {
				fragment: Some(plan
					.view
					.name
					.clone()
					.into_owned()),
				name: plan.view.name.text().to_string(),
				schema: plan.schema.id,
				columns: plan.columns,
			},
		)?;

		self.create_flow(txn, &result, plan.with)?;

		Ok(Columns::single_row([
			("schema", Value::Utf8(plan.schema.name.to_string())),
			(
				"view",
				Value::Utf8(plan.view.name.text().to_string()),
			),
			("created", Value::Boolean(true)),
		]))
	}
}
