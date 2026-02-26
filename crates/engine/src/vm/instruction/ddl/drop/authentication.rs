// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::DropAuthenticationNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn drop_authentication(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: DropAuthenticationNode,
) -> crate::Result<Columns> {
	let user_name = plan.user.text();
	let method = plan.method.text();

	// Find the user
	let user = if plan.if_exists {
		match services.catalog.find_user_by_name(&mut Transaction::Admin(&mut *txn), user_name)? {
			Some(u) => u,
			None => {
				return Ok(Columns::single_row([
					("user", Value::Utf8(user_name.to_string())),
					("method", Value::Utf8(method.to_string())),
					("dropped", Value::Boolean(false)),
				]));
			}
		}
	} else {
		services.catalog.get_user_by_name(&mut Transaction::Admin(&mut *txn), user_name)?
	};

	// Drop the authentication
	services.catalog.drop_user_authentication(txn, user.id, method)?;

	Ok(Columns::single_row([
		("user", Value::Utf8(user_name.to_string())),
		("method", Value::Utf8(method.to_string())),
		("dropped", Value::Boolean(true)),
	]))
}
