// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_auth::error::AuthError;
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateAuthenticationNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::{error::Error, value::Value};

use crate::{Result, vm::services::Services};

pub(crate) fn create_authentication(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateAuthenticationNode,
) -> Result<Columns> {
	let user_name = plan.user.text();
	let method = plan.method.text();

	// Find the user
	let user = services.catalog.get_user_by_name(&mut Transaction::Admin(&mut *txn), user_name)?;

	// Get the auth provider
	let provider = services.auth_registry.get(method).ok_or_else(|| {
		Error::from(AuthError::UnknownMethod {
			method: method.to_string(),
		})
	})?;

	// Create the authentication properties
	let properties = provider.create(&services.runtime_context.rng, &plan.config)?;

	// Extract token for response (if token method)
	let token_value = properties.get("token").cloned();

	// Store in catalog
	services.catalog.create_authentication(txn, user.id, method, properties)?;

	let mut row: Vec<(&str, Value)> = vec![
		("user", Value::Utf8(user_name.to_string())),
		("method", Value::Utf8(method.to_string())),
		("created", Value::Boolean(true)),
	];
	if let Some(token) = token_value {
		row.push(("token", Value::Utf8(token)));
	}

	Ok(Columns::single_row(row))
}
