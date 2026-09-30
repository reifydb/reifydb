// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::id::SubscriptionId, change::StagedBatch},
};
use reifydb_engine::subscription::{HydrateError, SubscriptionServiceRef};
use reifydb_transaction::multi::lease::VersionLeaseGuard;
use reifydb_value::value::{duration::Duration, identity::IdentityId};
#[cfg(not(reifydb_single_threaded))]
use tokio::task::spawn_blocking;

pub fn run_hydrate_sync(
	service: SubscriptionServiceRef,
	subscription_id: SubscriptionId,
	identity: IdentityId,
	lease: VersionLeaseGuard,
	max_rows: u64,
) -> Result<(CommitVersion, Vec<StagedBatch>, Duration), HydrateError> {
	let outcome = service.hydrate(subscription_id, identity, lease, max_rows)?;

	Ok((outcome.version, outcome.batches, outcome.total))
}

#[cfg(not(reifydb_single_threaded))]
pub async fn run_hydrate(
	service: SubscriptionServiceRef,
	subscription_id: SubscriptionId,
	identity: IdentityId,
	lease: VersionLeaseGuard,
	max_rows: u64,
) -> Result<(CommitVersion, Vec<StagedBatch>, Duration), HydrateError> {
	spawn_blocking(move || run_hydrate_sync(service, subscription_id, identity, lease, max_rows))
		.await
		.map_err(|e| HydrateError::Internal(e.to_string()))?
}
