// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{view::ViewToCreate, Catalog};
use reifydb_core::{
	result::error::diagnostic::catalog::view_already_exists,
	return_error, Value,
};
use reifydb_rql::plan::physical::CreateTransactionalViewPlan;

use crate::{columnar::Columns, execute::{Executor, FullCommandTransaction}};

impl Executor {
	pub(crate) fn create_transactional_view<CT: FullCommandTransaction<CT>>(
		&self,
		txn: &mut CT,
		plan: CreateTransactionalViewPlan,
	) -> crate::Result<Columns> {
		let catalog = Catalog::new();
		if let Some(view) = catalog.find_view_by_name(
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

		let result = catalog.create_transactional_view(
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
