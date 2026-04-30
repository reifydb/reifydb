// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{event::EventBus, interface::catalog::id::NamespaceId};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::interceptors::Interceptors, multi::transaction::MultiTransaction, single::SingleTransaction,
	transaction::admin::AdminTransaction,
};
use reifydb_type::value::identity::IdentityId;

use super::ensure_namespace;
use crate::{Result, catalog::Catalog, materialized::MaterializedCatalog};

/// Create the `system::bindings` namespace (persistent, idempotent).
/// Binding catalog rows themselves are persisted via `Catalog::create_binding`
/// and loaded from storage by `MaterializedCatalogLoader::load_bindings`.
pub fn bootstrap_system_bindings(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &MaterializedCatalog,
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
		NamespaceId::SYSTEM_BINDINGS,
		"system::bindings",
		"bindings",
		NamespaceId::SYSTEM,
	)?;

	admin.commit()?;
	Ok(())
}
