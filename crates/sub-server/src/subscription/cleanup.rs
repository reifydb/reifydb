// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(not(reifydb_single_threaded))]
use reifydb_core::error::diagnostic::internal::internal;
use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_engine::engine::StandardEngine;
#[cfg(not(reifydb_single_threaded))]
use reifydb_value::error::Error;
use reifydb_value::{Result as TypeResult, params::Params, value::identity::IdentityId};
#[cfg(not(reifydb_single_threaded))]
use tokio::task::spawn_blocking;

#[cfg(not(reifydb_single_threaded))]
use crate::state::AppState;

pub fn cleanup_subscription_sync(engine: &StandardEngine, subscription_id: SubscriptionId) -> TypeResult<()> {
	let rql = format!("drop subscription if exists subscription_{};", subscription_id.0);
	engine.admin_as(IdentityId::system(), &rql, Params::None).check()?;
	Ok(())
}

#[cfg(not(reifydb_single_threaded))]
pub async fn cleanup_subscription(state: &AppState, subscription_id: SubscriptionId) -> TypeResult<()> {
	let engine = state.engine_clone();

	spawn_blocking(move || cleanup_subscription_sync(&engine, subscription_id))
		.await
		.map_err(|e| Error(Box::new(internal(format!("Blocking task error: {:?}", e)))))?
}
