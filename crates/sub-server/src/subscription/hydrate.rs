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
use tokio::task::spawn_blocking;

pub async fn run_hydrate(
	service: SubscriptionServiceRef,
	engine: StandardEngine,
	subscription_id: SubscriptionId,
	identity: IdentityId,
	lease: VersionLeaseGuard,
	max_rows: u64,
) -> Result<(CommitVersion, Vec<StagedBatch>, ExecutionMetrics), HydrateError> {
	let outcome = spawn_blocking(move || service.hydrate(subscription_id, &engine, identity, lease, max_rows))
		.await
		.map_err(|e| HydrateError::Internal(e.to_string()))??;

	Ok((outcome.version, outcome.batches, outcome.metrics))
}
