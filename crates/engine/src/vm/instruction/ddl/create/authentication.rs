// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_auth::error::AuthError;
use reifydb_catalog::error::CatalogError;
use reifydb_core::value::column::columns::Columns;
use reifydb_rql::nodes::CreateAuthenticationNode;
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	error::Error,
	value::{Value, identity::IdentityKind},
};

use crate::{Result, vm::services::Services};

const PASSWORD_METHOD: &str = "password";

pub(crate) fn create_authentication(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateAuthenticationNode,
) -> Result<Columns> {
	let user_name = plan.user.text();
	let method = plan.method.text();

	let user = services.catalog.get_identity_by_name(&mut Transaction::Admin(&mut *txn), user_name)?;

	if user.resolved_kind() == IdentityKind::Service && method == PASSWORD_METHOD {
		return Err(CatalogError::IdentityKindInvalid {
			name: user_name.to_string(),
			reason: "a service cannot hold a password credential".to_string(),
			fragment: plan.user.clone(),
		}
		.into());
	}

	let provider = services.auth_registry.get(method).ok_or_else(|| {
		Error::from(AuthError::UnknownMethod {
			method: method.to_string(),
		})
	})?;

	let properties = provider.create(&services.runtime_context.rng, &plan.config)?;

	let token_value = properties.get("token").cloned();

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
