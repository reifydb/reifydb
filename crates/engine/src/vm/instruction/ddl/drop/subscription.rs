// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::catalog::subscription_not_found,
	interface::catalog::{
		id::{NamespaceId, SubscriptionId},
		subscription::subscription_flow_name,
	},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::DropSubscriptionNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{return_error, value::Value};

use crate::{Result, vm::services::Services};

pub(crate) fn drop_subscription(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropSubscriptionNode,
) -> Result<Columns> {
	let name = plan.subscription_name.text();

	// Parse the subscription ID from the name (convention: "subscription_<id>")
	let id = name.strip_prefix("subscription_").and_then(|s| s.parse::<u64>().ok()).map(SubscriptionId);

	// Look up the subscription in the catalog
	let subscription = match id {
		Some(sub_id) => {
			let subs = services.catalog.list_subscriptions_all(&mut Transaction::Admin(txn))?;
			subs.into_iter().find(|s| s.id == sub_id)
		}
		None => None,
	};

	let Some(subscription) = subscription else {
		if plan.if_exists {
			return Ok(Columns::single_row([
				("subscription", Value::Utf8(name.to_string())),
				("dropped", Value::Boolean(false)),
			]));
		}
		return_error!(subscription_not_found(plan.subscription_name.clone(), name));
	};

	// Drop the subscription itself
	services.catalog.drop_subscription(txn, subscription.clone())?;

	// Also drop the associated flow (created in the system namespace)
	let flow_name = subscription_flow_name(subscription.id);
	let flows = services.catalog.list_flows_all(&mut Transaction::Admin(txn))?;
	if let Some(flow) = flows.iter().find(|f| f.namespace == NamespaceId(1) && f.name == flow_name) {
		services.catalog.drop_flow(txn, flow.clone())?;
	}

	Ok(Columns::single_row([("subscription", Value::Utf8(name.to_string())), ("dropped", Value::Boolean(true))]))
}
