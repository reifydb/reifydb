// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FlowChangeProvider - shared, cached layer for CDC-to-FlowChange conversion.
//!
//! Instead of each FlowConsumer independently fetching and decoding CDC data,
//! this provider centralizes fetching and caches decoded results by version.
//! Flow consumers request changes on-demand, and the provider handles caching
//! with request coalescing to prevent duplicate fetches.

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
};

use reifydb_core::{
	CommitVersion,
	interface::{CdcChange, PrimitiveId},
	key::Key,
	util::LruCache,
};
use reifydb_engine::StandardEngine;
use reifydb_sdk::FlowChange;
use reifydb_transaction::cdc::CdcQueryTransaction;
use tokio::sync::{Mutex, RwLock, watch};
use tracing::warn;

use crate::{catalog::FlowCatalog, convert::convert_cdc_to_flow_change};

/// All decoded changes for a single commit version.
pub type VersionChanges = Vec<FlowChange>;

/// Cache entry: changes + set of affected primitives.
#[derive(Clone)]
struct CachedVersion {
	changes: Arc<VersionChanges>,
	primitives: HashSet<PrimitiveId>,
}

/// Shared provider for decoded FlowChanges.
///
/// This is a passive component - it does not poll. It serves and stores
/// changes on-demand when consumers request them.
///
/// Thread-safe: Multiple flow consumers can request changes concurrently.
pub struct FlowChangeProvider {
	/// Engine for fetching CDC and creating transactions.
	engine: StandardEngine,

	/// LRU cache: version -> decoded changes + affected primitives.
	/// Protected by Mutex for async-safe access with interior mutability.
	cache: Mutex<LruCache<CommitVersion, CachedVersion>>,

	/// In-flight fetches: prevents duplicate fetches for same version.
	/// Maps version to a channel that will receive the result.
	in_flight: RwLock<HashMap<CommitVersion, watch::Receiver<Option<CachedVersion>>>>,

	/// Catalog cache for source metadata (shared across fetches).
	catalog_cache: FlowCatalog,
}

impl FlowChangeProvider {
	/// Create a new FlowChangeProvider.
	pub fn new(engine: StandardEngine) -> Self {
		let catalog_cache = FlowCatalog::new(engine.catalog());

		Self {
			engine,
			cache: Mutex::new(LruCache::new(10_000)),
			in_flight: RwLock::new(HashMap::new()),
			catalog_cache,
		}
	}

	/// Get decoded changes for a specific version if any sources match.
	///
	/// Returns `Ok(None)` if no changes affect the provided sources.
	/// On cache hit: checks primitives intersection and returns immediately.
	/// On cache miss: fetches CDC, decodes, caches, and returns.
	///
	/// Concurrent requests for the same version will coalesce (only one fetch).
	pub async fn get_changes(
		&self,
		version: CommitVersion,
		sources: &HashSet<PrimitiveId>,
	) -> crate::Result<Option<Arc<VersionChanges>>> {
		// Fast path: check cache
		{
			let mut cache = self.cache.lock().await;
			if let Some(cached) = cache.get(&version) {
				// Check if any sources intersect with cached primitives
				if cached.primitives.is_disjoint(sources) {
					return Ok(None);
				}
				return Ok(Some(Arc::clone(&cached.changes)));
			}
		}

		// Slow path: need to fetch and decode
		self.fetch_and_cache(version, sources).await
	}

	/// Internal: fetch CDC, decode, and cache with request coalescing.
	async fn fetch_and_cache(
		&self,
		version: CommitVersion,
		sources: &HashSet<PrimitiveId>,
	) -> crate::Result<Option<Arc<VersionChanges>>> {
		// Check if another task is already fetching this version
		{
			let in_flight = self.in_flight.read().await;
			if let Some(receiver) = in_flight.get(&version) {
				let mut rx = receiver.clone();
				drop(in_flight);

				rx.changed().await.ok();
				if let Some(cached) = rx.borrow().clone() {
					if cached.primitives.is_disjoint(sources) {
						return Ok(None);
					}
					return Ok(Some(cached.changes));
				}
			}
		}

		let (tx, rx) = watch::channel(None);
		{
			let mut in_flight = self.in_flight.write().await;

			// Double-check: another task might have started while we were waiting for write lock
			if let Some(receiver) = in_flight.get(&version) {
				let mut rx = receiver.clone();
				drop(in_flight);

				rx.changed().await.ok();
				if let Some(cached) = rx.borrow().clone() {
					if cached.primitives.is_disjoint(sources) {
						return Ok(None);
					}
					return Ok(Some(cached.changes));
				}
				// Re-acquire write lock after checking
				let mut in_flight = self.in_flight.write().await;
				in_flight.insert(version, rx);
			} else {
				in_flight.insert(version, rx);
			}
		}

		// Perform the actual fetch and decode
		let result = self.do_fetch_and_decode(version).await;

		// Store result and clean up
		match result {
			Ok((changes, primitives)) => {
				let arc_changes = Arc::new(changes);
				let cached = CachedVersion {
					changes: Arc::clone(&arc_changes),
					primitives: primitives.clone(),
				};

				// Cache the result
				{
					let mut cache = self.cache.lock().await;
					cache.put(
						version,
						CachedVersion {
							changes: Arc::clone(&arc_changes),
							primitives: primitives.clone(),
						},
					);
				}

				// Notify waiters
				tx.send(Some(cached)).ok();

				// Clean up in-flight
				{
					let mut in_flight = self.in_flight.write().await;
					in_flight.remove(&version);
				}

				// Check intersection for current request
				if primitives.is_disjoint(sources) {
					Ok(None)
				} else {
					Ok(Some(arc_changes))
				}
			}
			Err(e) => {
				// Clean up in-flight on error (don't cache failures)
				let mut in_flight = self.in_flight.write().await;
				in_flight.remove(&version);
				Err(e)
			}
		}
	}

	/// Actually fetch CDC and decode to FlowChanges.
	/// Returns changes and the set of primitives that were affected.
	async fn do_fetch_and_decode(
		&self,
		version: CommitVersion,
	) -> crate::Result<(VersionChanges, HashSet<PrimitiveId>)> {
		// Begin query transaction at the version for dictionary decoding
		let mut query_txn = self.engine.begin_query_at_version(version).await?;

		// Fetch CDC for this specific version
		let cdc_txn = query_txn.begin_cdc_query().await?;
		let cdc_opt = cdc_txn.get(version).await?;

		let cdc = match cdc_opt {
			Some(cdc) => cdc,
			None => {
				// No CDC at this version - return empty
				return Ok((Vec::new(), HashSet::new()));
			}
		};

		// Decode all changes and track affected primitives
		let mut all_changes = Vec::new();
		let mut primitives = HashSet::new();

		for cdc_change in &cdc.changes {
			// Only process Row keys (data events)
			if let Some(Key::Row(row_key)) = Key::decode(cdc_change.key()) {
				let source_id = row_key.primitive;
				let row_number = row_key.row;

				// Track this primitive
				primitives.insert(source_id);

				// Skip Delete events with no pre-image (would result in empty encoded values)
				if let CdcChange::Delete {
					pre: None,
					..
				} = &cdc_change.change
				{
					continue;
				}

				match convert_cdc_to_flow_change(
					&mut query_txn,
					&self.catalog_cache,
					source_id,
					row_number,
					&cdc_change.change,
					version,
				)
				.await
				{
					Ok(change) => {
						all_changes.push(change);
					}
					Err(e) => {
						warn!(
							source = ?source_id,
							row = row_number.0,
							error = %e,
							"failed to decode row in provider"
						);
						continue;
					}
				}
			}
		}

		Ok((all_changes, primitives))
	}
}
