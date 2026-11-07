// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::event::EventBus;
use reifydb_runtime::context::{RuntimeContext, transaction::TransactionContext};
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{Transaction, admin::AdminTransaction, query::QueryTransaction},
};
use reifydb_value::value::identity::{IdentityId, IdentityKind};

use crate::{
	CatalogStore, Result,
	cache::{CatalogCache, load::identity::load_identities},
};

pub fn bootstrap_identities(
	multi: &MultiTransaction,
	single: &SingleTransaction,
	catalog: &CatalogCache,
	eventbus: &EventBus,
	runtime: &RuntimeContext,
) -> Result<()> {
	let mut qt = QueryTransaction::new(
		multi.begin_query(multi.generate_transaction_id())?,
		single.clone(),
		Arc::new(TransactionContext::system(runtime.clone())),
	);
	let mut missing = Vec::new();
	for (name, id, kind) in [
		("root", IdentityId::root(), IdentityKind::Root),
		("system", IdentityId::system(), IdentityKind::System),
	] {
		if CatalogStore::find_identity_by_name(&mut Transaction::Query(&mut qt), name)?.is_none() {
			missing.push((name, id, kind));
		}
	}
	drop(qt);
	if missing.is_empty() {
		return Ok(());
	}

	let mut admin = AdminTransaction::new(
		multi.begin_command(multi.generate_transaction_id())?,
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		Arc::new(TransactionContext::system(runtime.clone())),
	);

	for (name, id, kind) in missing {
		CatalogStore::create_identity_with_id(&mut admin, name, id, kind)?;
	}
	admin.commit()?;

	let mut qt = QueryTransaction::new(
		multi.begin_query(multi.generate_transaction_id())?,
		single.clone(),
		Arc::new(TransactionContext::system(runtime.clone())),
	);
	load_identities(&mut Transaction::Query(&mut qt), catalog)?;

	Ok(())
}
