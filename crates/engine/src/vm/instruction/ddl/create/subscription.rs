// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::subscription::SubscriptionToCreate;
use reifydb_core::{
	interface::catalog::change::CatalogTrackSubscriptionChangeOperations, value::column::columns::Columns,
};
use reifydb_rql::nodes::CreateSubscriptionNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use super::create_subscription_flow;
use crate::{Result, vm::services::Services};

pub(crate) fn create_subscription(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateSubscriptionNode,
) -> Result<Columns> {
	let result = services.catalog.create_subscription(
		txn,
		SubscriptionToCreate {
			columns: plan.columns,
		},
	)?;
	txn.track_subscription_def_created(result.clone())?;

	if let Some(as_clause) = plan.as_clause {
		create_subscription_flow(&services.catalog, txn, &result, *as_clause)?;
	}

	Ok(Columns::single_row([("subscription_id", Value::Uint8(result.id.0)), ("created", Value::Boolean(true))]))
}
