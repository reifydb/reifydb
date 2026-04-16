// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Bootstrap the root identity. Creates a catalog identity named `root` with
//! `IdentityId::root()` so that authentication (tokens, etc.) can be attached to it.

use reifydb_core::event::EventBus;
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction},
};
use reifydb_type::value::identity::IdentityId;

use crate::{
	CatalogStore, Result,
	materialized::{MaterializedCatalog, load::identity::load_identities},
};

/// Bootstrap the root identity in the catalog.
///
/// Creates an identity named "root" with `IdentityId::root()`.
/// This makes root a real catalog identity that can have authentication attached
/// (e.g., `CREATE AUTHENTICATION FOR root { method: token; token: '...' }`).
///
/// Skips creation if the root identity already exists.
pub fn bootstrap_root_identity(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &MaterializedCatalog,
	eventbus: &EventBus,
) -> Result<()> {
	let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone(), IdentityId::system());
	if CatalogStore::find_identity_by_name(&mut Transaction::Query(&mut qt), "root")?.is_some() {
		return Ok(());
	}
	drop(qt);

	let mut admin = AdminTransaction::new(
		multi.clone(),
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		IdentityId::system(),
		Clock::Real,
	)?;

	CatalogStore::create_identity_with_id(&mut admin, "root", IdentityId::root())?;
	admin.commit()?;

	// Reload materialized catalog to pick up the new identity
	let mut qt = QueryTransaction::new(multi.begin_query()?, single.clone(), IdentityId::system());
	load_identities(&mut Transaction::Query(&mut qt), catalog)?;

	Ok(())
}
