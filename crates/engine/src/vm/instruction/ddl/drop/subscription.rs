// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropSubscriptionNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn drop_subscription(
	_services: &Services,
	_txn: &mut AdminTransaction,
	plan: DropSubscriptionNode,
) -> crate::Result<Columns> {
	// Subscriptions use ID-based lookup; the name-based drop is not yet wired through the catalog.
	// For now, return the result structure. Full subscription drop support requires
	// parsing the subscription ID from the name.
	Ok(Columns::single_row([
		("subscription", Value::Utf8(plan.subscription_name.text().to_string())),
		("dropped", Value::Boolean(false)),
	]))
}
