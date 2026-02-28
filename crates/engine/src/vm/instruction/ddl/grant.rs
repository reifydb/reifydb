// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::GrantNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn grant(services: &Services, txn: &mut AdminTransaction, plan: GrantNode) -> Result<Columns> {
	let role_name = plan.role.text();
	let user_name = plan.user.text();

	let role = services.catalog.get_role_by_name(&mut Transaction::Admin(&mut *txn), role_name)?;
	let user = services.catalog.get_user_by_name(&mut Transaction::Admin(&mut *txn), user_name)?;

	services.catalog.grant_role(txn, user.id, role.id)?;

	Ok(Columns::single_row([
		("role", Value::Utf8(role_name.to_string())),
		("user", Value::Utf8(user_name.to_string())),
		("granted", Value::Boolean(true)),
	]))
}
