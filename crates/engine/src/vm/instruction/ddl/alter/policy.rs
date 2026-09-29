// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::value::batch::single_row;
use reifydb_rql::nodes::AlterPolicyNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn alter_policy(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: AlterPolicyNode,
) -> Result<RecordBatch> {
	let name = plan.name.text();

	let policy = services.catalog.get_policy_by_name(&mut Transaction::Admin(&mut *txn), name)?;

	services.catalog.alter_policy(txn, policy.id, plan.enable)?;

	single_row([("policy", Value::Utf8(name.to_string())), ("altered", Value::Boolean(true))])
}
