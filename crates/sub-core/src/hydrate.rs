// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::id::SubscriptionId, change::StagedBatch},
	metrics::execution::ExecutionMetrics,
};
use reifydb_engine::{
	engine::StandardEngine,
	subscription::{HydrateError, SubscriptionServiceRef},
};
use reifydb_transaction::multi::lease::VersionLeaseGuard;
use reifydb_value::value::identity::IdentityId;
#[cfg(not(reifydb_single_threaded))]
use tokio::task::spawn_blocking;

pub fn run_hydrate_sync(
	service: SubscriptionServiceRef,
	engine: StandardEngine,
	subscription_id: SubscriptionId,
	identity: IdentityId,
	lease: VersionLeaseGuard,
	max_rows: u64,
) -> Result<(CommitVersion, Vec<StagedBatch>, ExecutionMetrics), HydrateError> {
	let outcome = service.hydrate(subscription_id, &engine, identity, lease, max_rows)?;

	Ok((outcome.version, outcome.batches, outcome.metrics))
}

#[cfg(not(reifydb_single_threaded))]
pub async fn run_hydrate(
	service: SubscriptionServiceRef,
	engine: StandardEngine,
	subscription_id: SubscriptionId,
	identity: IdentityId,
	lease: VersionLeaseGuard,
	max_rows: u64,
) -> Result<(CommitVersion, Vec<StagedBatch>, ExecutionMetrics), HydrateError> {
	spawn_blocking(move || run_hydrate_sync(service, engine, subscription_id, identity, lease, max_rows))
		.await
		.map_err(|e| HydrateError::Internal(e.to_string()))?
}
