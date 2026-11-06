// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{event::EventBus, interface::catalog::id::NamespaceId};
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
	)?;

	ensure_namespace(
		&catalog_api,
		&mut admin,
		NamespaceId::SYSTEM_METRIC_STORE,
		"system::metric::store",
		"store",
		NamespaceId::SYSTEM_METRIC,
	)?;

	for (id, path, local_name, parent) in STORE_NAMESPACES {
		ensure_namespace(&catalog_api, &mut admin, id, path, local_name, parent)?;
	}

	admin.commit()?;
	Ok(())
}

const STORE_NAMESPACES: [(NamespaceId, &str, &str, NamespaceId); 12] = [
	(
		NamespaceId::SYSTEM_METRIC_STORE_MULTI,
		"system::metric::store::multi",
		"multi",
		NamespaceId::SYSTEM_METRIC_STORE,
	),
	(
		NamespaceId::SYSTEM_METRIC_STORE_MULTI_COMMIT,
		"system::metric::store::multi::commit",
		"commit",
		NamespaceId::SYSTEM_METRIC_STORE_MULTI,
	),
	(
		NamespaceId::SYSTEM_METRIC_STORE_MULTI_RANGE,
		"system::metric::store::multi::range",
		"range",
		NamespaceId::SYSTEM_METRIC_STORE_MULTI,
	),
	(
		NamespaceId::SYSTEM_METRIC_STORE_MULTI_POINT,
		"system::metric::store::multi::point",
		"point",
		NamespaceId::SYSTEM_METRIC_STORE_MULTI,
	),
	(
		NamespaceId::SYSTEM_METRIC_STORE_MULTI_PERSISTENT,
		"system::metric::store::multi::persistent",
		"persistent",
		NamespaceId::SYSTEM_METRIC_STORE_MULTI,
	),
	(
		NamespaceId::SYSTEM_METRIC_STORE_SINGLE,
		"system::metric::store::single",
		"single",
		NamespaceId::SYSTEM_METRIC_STORE,
	),
	(
		NamespaceId::SYSTEM_METRIC_STORE_SINGLE_COMMIT,
		"system::metric::store::single::commit",
		"commit",
		NamespaceId::SYSTEM_METRIC_STORE_SINGLE,
	),
	(
		NamespaceId::SYSTEM_METRIC_STORE_SINGLE_PERSISTENT,
		"system::metric::store::single::persistent",
		"persistent",
		NamespaceId::SYSTEM_METRIC_STORE_SINGLE,
	),
	(
		NamespaceId::SYSTEM_METRIC_STORE_OPERATOR,
		"system::metric::store::operator",
		"operator",
		NamespaceId::SYSTEM_METRIC_STORE,
	),
	(
		NamespaceId::SYSTEM_METRIC_STORE_OPERATOR_RANGE,
		"system::metric::store::operator::range",
		"range",
		NamespaceId::SYSTEM_METRIC_STORE_OPERATOR,
	),
	(
		NamespaceId::SYSTEM_METRIC_STORE_OPERATOR_RANGE_KEYSPACE,
		"system::metric::store::operator::range::keyspace",
		"keyspace",
		NamespaceId::SYSTEM_METRIC_STORE_OPERATOR_RANGE,
	),
	(
		NamespaceId::SYSTEM_METRIC_STORE_OPERATOR_PERSISTENT,
		"system::metric::store::operator::persistent",
		"persistent",
		NamespaceId::SYSTEM_METRIC_STORE_OPERATOR,
	),
];
