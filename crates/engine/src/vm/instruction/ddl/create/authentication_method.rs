// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use reifydb_catalog::error::CatalogError;
use reifydb_core::{
	interface::{
		auth::{AuthenticationProvider, MethodSettingError},
		catalog::authentication_method::AuthenticationMethodToCreate,
	},
	value::batch::single_row,
};
use reifydb_rql::nodes::CreateAuthenticationMethodNode;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::Value;

use crate::{Result, vm::services::Services};

pub(crate) fn create_authentication_method(
	services: &Services,
	txn: &mut AdminTransaction,
	plan: CreateAuthenticationMethodNode,
) -> Result<RecordBatch> {
	let name = plan.name.text();
	let provider = registered_provider(services, name)?;
	let properties = configure_method(provider, name, &plan.properties)?;
	check_promote_guest(provider, name, plan.promote_guest)?;

	services.catalog.create_authentication_method(
		txn,
		AuthenticationMethodToCreate {
			name: name.to_string(),
			ttl: plan.ttl,
			promote_guest: plan.promote_guest,
			properties,
		},
	)?;

	single_row([("method", Value::Utf8(name.to_string())), ("created", Value::Boolean(true))])
}

pub(crate) fn registered_provider<'a>(services: &'a Services, name: &str) -> Result<&'a dyn AuthenticationProvider> {
	services.auth_registry.get(name).ok_or_else(|| {
		CatalogError::UnknownAuthenticationProvider {
			name: name.to_string(),
			registered: services.auth_registry.names().into_iter().map(str::to_string).collect(),
		}
		.into()
	})
}

pub(crate) fn configure_method(
	provider: &dyn AuthenticationProvider,
	name: &str,
	properties: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
	provider.configure_method(properties).map_err(|err| {
		match err {
			MethodSettingError::Unknown {
				key,
			} => CatalogError::UnknownMethodSetting {
				method: name.to_string(),
				key,
			},
			MethodSettingError::Missing {
				key,
			} => CatalogError::MissingMethodSetting {
				method: name.to_string(),
				key,
			},
			MethodSettingError::HostSecretMissing {
				key,
			} => CatalogError::AuthenticationMethodHostSecretMissing {
				method: name.to_string(),
				key,
			},
		}
		.into()
	})
}

pub(crate) fn check_promote_guest(
	provider: &dyn AuthenticationProvider,
	name: &str,
	promote_guest: bool,
) -> Result<()> {
	if promote_guest && !provider.can_promote_guest() {
		return Err(CatalogError::PromoteGuestNotAllowed {
			method: name.to_string(),
		}
		.into());
	}
	Ok(())
}
