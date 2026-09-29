// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::value::batch::single_row;
use reifydb_rql::nodes::RevokeNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn revoke(services: &Services, txn: &mut AdminTransaction, plan: RevokeNode) -> Result<RecordBatch> {
	let role_name = plan.role.text();
	let user_name = plan.user.text();

	let role = services.catalog.get_role_by_name(&mut Transaction::Admin(&mut *txn), role_name)?;
	let identity = services.catalog.get_identity_by_name(&mut Transaction::Admin(&mut *txn), user_name)?;

	services.catalog.revoke_role(txn, identity.id, role.id)?;

	single_row([
		("role", Value::Utf8(role_name.to_string())),
		("identity", Value::Utf8(user_name.to_string())),
		("revoked", Value::Boolean(true)),
	])
}
