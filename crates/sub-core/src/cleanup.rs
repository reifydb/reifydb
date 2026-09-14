// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{error::diagnostic::internal::internal, interface::catalog::id::SubscriptionId};
use reifydb_engine::{engine::StandardEngine, subscription::SubscriptionServiceRef};
use reifydb_value::{
	Result as TypeResult,
	error::{Diagnostic, Error},
};
#[cfg(not(reifydb_single_threaded))]
use tokio::task::spawn_blocking;

pub fn cleanup_subscription_sync(engine: &StandardEngine, subscription_id: SubscriptionId) -> TypeResult<()> {
	unregister(engine, subscription_id).map_err(|cause| {
		Error(Box::new(Diagnostic {
			cause: Some(cause.0),
			..internal(format!("could not unregister subscription {}", subscription_id))
		}))
	})
}

fn unregister(engine: &StandardEngine, subscription_id: SubscriptionId) -> TypeResult<()> {
	engine.ioc().resolve::<SubscriptionServiceRef>()?.unregister_subscription(&subscription_id)?;
	Ok(())
}

#[cfg(not(reifydb_single_threaded))]
pub async fn cleanup_subscription(engine: &StandardEngine, subscription_id: SubscriptionId) -> TypeResult<()> {
	let engine = engine.clone();

	spawn_blocking(move || cleanup_subscription_sync(&engine, subscription_id)).await.map_err(|e| {
		Error(Box::new(internal(format!(
			"could not unregister subscription {}: blocking task error: {:?}",
			subscription_id, e
		))))
	})?
}
