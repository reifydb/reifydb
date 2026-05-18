// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::catalog::subscription_not_found, interface::catalog::id::SubscriptionId,
	value::column::columns::Columns,
};
use reifydb_rql::nodes::DropSubscriptionNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{error::Error, value::Value};

use crate::{Result, subscription::SubscriptionServiceRef, vm::services::Services};

pub(crate) fn drop_subscription(
	services: &Services,
	_txn: &mut Transaction<'_>,
	plan: DropSubscriptionNode,
) -> Result<Columns> {
	let name = plan.subscription_name.text();

	let id = name.strip_prefix("subscription_").and_then(|s| s.parse::<u64>().ok()).map(SubscriptionId);

	let dropped = match (id, services.ioc.resolve::<SubscriptionServiceRef>().ok()) {
		(Some(sub_id), Some(sub_service)) => sub_service.unregister_subscription(&sub_id).is_ok(),
		_ => false,
	};

	if !dropped && !plan.if_exists {
		return Err(Error(Box::new(subscription_not_found(plan.subscription_name.clone(), name))));
	}

	Ok(Columns::single_row([("subscription", Value::Utf8(name.to_string())), ("dropped", Value::Boolean(dropped))]))
}
