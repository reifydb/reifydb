// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::subscription::SubscriptionToCreate;
use reifydb_core::{
	interface::catalog::change::CatalogTrackSubscriptionChangeOperations, value::column::columns::Columns,
};
use reifydb_rql::{nodes::CreateSubscriptionNode, query::QueryPlan};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use super::create_subscription_flow;
use crate::{Result, vm::services::Services};

pub(crate) fn create_subscription(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateSubscriptionNode,
) -> Result<Columns> {
	// Check if the plan targets a remote source
	if let Some(ref as_clause) = plan.as_clause {
		if let QueryPlan::RemoteScan(ref remote) = **as_clause {
			return Ok(Columns::single_row([
				("remote_address", Value::Utf8(remote.address.clone())),
				("remote_rql", Value::Utf8(remote.remote_rql.clone())),
			]));
		}
	}

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
