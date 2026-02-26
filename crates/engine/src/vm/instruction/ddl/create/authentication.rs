// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_auth::error::AuthError;
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateAuthenticationNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::Value;

use crate::vm::services::Services;

pub(crate) fn create_authentication(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateAuthenticationNode,
) -> crate::Result<Columns> {
	let user_name = plan.user.text();
	let method = plan.method.text();

	// Find the user
	let user = services.catalog.get_user_by_name(&mut Transaction::Admin(&mut *txn), user_name)?;

	// Get the auth provider
	let provider = services.auth_registry.get(method).ok_or_else(|| {
		reifydb_type::error::Error::from(AuthError::UnknownMethod {
			method: method.to_string(),
		})
	})?;

	// Create the authentication properties
	let properties = provider.create(&plan.config)?;

	// Store in catalog
	services.catalog.create_user_authentication(txn, user.id, method, properties)?;

	Ok(Columns::single_row([
		("user", Value::Utf8(user_name.to_string())),
		("method", Value::Utf8(method.to_string())),
		("created", Value::Boolean(true)),
	]))
}
