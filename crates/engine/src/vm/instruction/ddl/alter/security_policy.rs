// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::AlterSecurityPolicyNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn alter_security_policy(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: AlterSecurityPolicyNode,
) -> crate::Result<Columns> {
	let name = plan.name.text();

	let policy = services.catalog.get_security_policy_by_name(&mut Transaction::Admin(&mut *txn), name)?;

	services.catalog.alter_security_policy(txn, policy.id, plan.enable)?;

	Ok(Columns::single_row([("policy", Value::Utf8(name.to_string())), ("altered", Value::Boolean(true))]))
}
