// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::subscription::SubscriptionToCreate;
use reifydb_core::{
	interface::catalog::change::CatalogTrackSubscriptionChangeOperations, value::column::columns::Columns,
};
use reifydb_rql::plan::physical::CreateSubscriptionNode;
use reifydb_transaction::standard::command::StandardCommandTransaction;
use reifydb_type::value::{Value, uuid::Uuid7};

use crate::execute::Executor;

impl Executor {
	pub(crate) fn create_subscription<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: CreateSubscriptionNode,
	) -> crate::Result<Columns> {
		let result = self.catalog.create_subscription(
			txn,
			SubscriptionToCreate {
				columns: plan.columns,
			},
		)?;
		txn.track_subscription_def_created(result.clone())?;

		// If AS clause is provided, create and compile a flow for the subscription
		if let Some(as_clause) = plan.as_clause {
			self.create_subscription_flow(txn, &result, *as_clause)?;
		}

		Ok(Columns::single_row([
			("subscription_id", Value::Uuid7(Uuid7(result.id.0))),
			("created", Value::Boolean(true)),
		]))
	}
}
