// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{event::EventBus, interface::catalog::id::NamespaceId};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::interceptors::Interceptors, multi::transaction::MultiTransaction, single::SingleTransaction,
	transaction::admin::AdminTransaction,
};
use reifydb_value::value::identity::IdentityId;

use super::ensure_namespace;
use crate::{Result, cache::CatalogCache, catalog::Catalog};

pub fn bootstrap_store(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &CatalogCache,
	eventbus: &EventBus,
) -> Result<()> {
	let catalog_api = Catalog::new(catalog.clone());
	let mut admin = AdminTransaction::new(
		multi.clone(),
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		IdentityId::system(),
		Clock::Real,
	)?;

	ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRICS_STORE,
		"system::metrics::store",
		"store",
		NamespaceId::SYSTEM_METRICS,
	)?;

	for (id, path, local_name, parent) in STORE_NAMESPACES {
		ensure_namespace(&catalog_api, &mut admin, id, path, local_name, parent)?;
	}

	admin.commit()?;
	Ok(())
}

const STORE_NAMESPACES: [(NamespaceId, &str, &str, NamespaceId); 10] = [
	(
		NamespaceId::SYSTEM_METRICS_STORE_MULTI,
		"system::metrics::store::multi",
		"multi",
		NamespaceId::SYSTEM_METRICS_STORE,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORE_MULTI_COMMIT,
		"system::metrics::store::multi::commit",
		"commit",
		NamespaceId::SYSTEM_METRICS_STORE_MULTI,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORE_MULTI_READ,
		"system::metrics::store::multi::read",
		"read",
		NamespaceId::SYSTEM_METRICS_STORE_MULTI,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORE_MULTI_PERSISTENT,
		"system::metrics::store::multi::persistent",
		"persistent",
		NamespaceId::SYSTEM_METRICS_STORE_MULTI,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORE_SINGLE,
		"system::metrics::store::single",
		"single",
		NamespaceId::SYSTEM_METRICS_STORE,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORE_SINGLE_COMMIT,
		"system::metrics::store::single::commit",
		"commit",
		NamespaceId::SYSTEM_METRICS_STORE_SINGLE,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORE_SINGLE_PERSISTENT,
		"system::metrics::store::single::persistent",
		"persistent",
		NamespaceId::SYSTEM_METRICS_STORE_SINGLE,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORE_OPERATOR,
		"system::metrics::store::operator",
		"operator",
		NamespaceId::SYSTEM_METRICS_STORE,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORE_OPERATOR_READ,
		"system::metrics::store::operator::read",
		"read",
		NamespaceId::SYSTEM_METRICS_STORE_OPERATOR,
	),
	(
		NamespaceId::SYSTEM_METRICS_STORE_OPERATOR_PERSISTENT,
		"system::metrics::store::operator::persistent",
		"persistent",
		NamespaceId::SYSTEM_METRICS_STORE_OPERATOR,
	),
];
