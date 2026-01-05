// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! FlowChangeProvider - shared, cached layer for CDC-to-FlowChange conversion.
//!
//! Instead of each FlowConsumer independently fetching and decoding CDC data,
//! this provider centralizes fetching and caches decoded results by version.
//! Flow consumers request changes on-demand, and the provider handles caching
//! with request coalescing to prevent duplicate fetches.

use std::{collections::HashSet, sync::Arc};

use broadcast::error::RecvError;
use dashmap::DashMap;
use reifydb_core::{
	CommitVersion,
	interface::{CdcChange, PrimitiveId},
	key::Key,
	util::LruCache,
};
use reifydb_engine::StandardEngine;
use reifydb_sdk::FlowChange;
use reifydb_transaction::cdc::CdcQueryTransaction;
use tokio::{
	select,
	sync::{Mutex, broadcast, watch},
};
use tokio_util::sync::CancellationToken;
use tracing::{Span, debug, instrument, warn};

use crate::{catalog::FlowCatalog, convert::convert_cdc_to_flow_change, coordinator::VersionBroadcast};

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
/// This component listens for version broadcasts from the coordinator and
/// pre-fetches CDC changes into cache before flow consumers request them.
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
	in_flight: DashMap<CommitVersion, watch::Receiver<Option<CachedVersion>>>,

	/// Catalog cache for source metadata (shared across fetches).
	catalog_cache: FlowCatalog,
}

impl FlowChangeProvider {
	/// Create and spawn a new FlowChangeProvider that pre-fetches versions.
	///
	/// The provider listens for version broadcasts and pre-fetches CDC changes
	/// into cache before flow consumers request them.
	pub fn spawn(
		engine: StandardEngine,
		version_rx: broadcast::Receiver<VersionBroadcast>,
		shutdown: CancellationToken,
	) -> Arc<Self> {
		let catalog_cache = FlowCatalog::new(engine.catalog());

		let provider = Arc::new(Self {
			engine,
			cache: Mutex::new(LruCache::new(10_000)),
			in_flight: DashMap::new(),
			catalog_cache,
		});

		// Spawn pre-fetch worker
		let provider_clone = Arc::clone(&provider);
		tokio::spawn(Self::prefetch_loop(provider_clone, version_rx, shutdown));

		debug!("flow change provider spawned");
		provider
	}

	/// Get decoded changes for a specific version if any sources match.
	///
	/// Returns `Ok(None)` if no changes affect the provided sources.
	/// On cache hit: checks primitives intersection and returns immediately.
	/// On cache miss: fetches CDC, decodes, caches, and returns.
	///
	/// Concurrent requests for the same version will coalesce (only one fetch).
	#[instrument(name = "flow::provider::get_changes", level = "debug", skip(self, sources), fields(
		version = version.0,
		sources_count = sources.len(),
		cache_hit = tracing::field::Empty,
		result = tracing::field::Empty,
	))]
	pub async fn get_changes(
		&self,
		version: CommitVersion,
		sources: &HashSet<PrimitiveId>,
	) -> crate::Result<Option<Arc<VersionChanges>>> {
		// Fast path: check cache
		{
			let cache = self.cache.lock().await;
			if let Some(cached) = cache.get(&version).await {
				Span::current().record("cache_hit", true);
				// Check if any sources intersect with cached primitives
				if cached.primitives.is_disjoint(sources) {
					Span::current().record("result", "cache_hit_disjoint");
					return Ok(None);
				}
				Span::current().record("result", "cache_hit");
				return Ok(Some(Arc::clone(&cached.changes)));
			}
		}
		Span::current().record("cache_hit", false);

		// Slow path: need to fetch and decode
		self.fetch_and_cache(version, sources).await
	}

	/// Internal: fetch CDC, decode, and cache with request coalescing.
	#[instrument(name = "flow::provider::fetch_and_cache", level = "debug", skip(self, sources), fields(
		version = version.0,
		coalesced = tracing::field::Empty,
	))]
	async fn fetch_and_cache(
		&self,
		version: CommitVersion,
		sources: &HashSet<PrimitiveId>,
	) -> crate::Result<Option<Arc<VersionChanges>>> {
		// Check if another task is already fetching this version
		if let Some(receiver) = self.in_flight.get(&version) {
			Span::current().record("coalesced", true);
			let mut rx = receiver.clone();
			drop(receiver); // Release DashMap ref before await

			rx.changed().await.ok();
			if let Some(cached) = rx.borrow().clone() {
				if cached.primitives.is_disjoint(sources) {
					return Ok(None);
				}
				return Ok(Some(cached.changes));
			}
		}

		let (tx, rx) = watch::channel(None);

		// Double-check: another task might have started while we were waiting
		if let Some(receiver) = self.in_flight.get(&version) {
			let mut rx = receiver.clone();
			drop(receiver); // Release DashMap ref before await

			rx.changed().await.ok();
			if let Some(cached) = rx.borrow().clone() {
				if cached.primitives.is_disjoint(sources) {
					return Ok(None);
				}
				return Ok(Some(cached.changes));
			}
		}

		self.in_flight.insert(version, rx);

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
					let cache = self.cache.lock().await;
					cache.put(
						version,
						CachedVersion {
							changes: Arc::clone(&arc_changes),
							primitives: primitives.clone(),
						},
					)
					.await;
				}

				// Notify waiters
				tx.send(Some(cached)).ok();

				// Clean up in-flight
				self.in_flight.remove(&version);

				// Check intersection for current request
				if primitives.is_disjoint(sources) {
					Ok(None)
				} else {
					Ok(Some(arc_changes))
				}
			}
			Err(e) => {
				// Clean up in-flight on error (don't cache failures)
				self.in_flight.remove(&version);
				Err(e)
			}
		}
	}

	/// Actually fetch CDC and decode to FlowChanges.
	/// Returns changes and the set of primitives that were affected.
	#[instrument(name = "flow::provider::fetch_and_decode", level = "debug", skip(self), fields(
		version = version.0,
		changes_count = tracing::field::Empty,
		primitives_count = tracing::field::Empty,
	))]
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

		Span::current().record("changes_count", all_changes.len());
		Span::current().record("primitives_count", primitives.len());

		Ok((all_changes, primitives))
	}

	/// Background loop that pre-fetches versions as broadcasts arrive.
	async fn prefetch_loop(
		provider: Arc<Self>,
		mut version_rx: broadcast::Receiver<VersionBroadcast>,
		shutdown: CancellationToken,
	) {
		debug!("provider prefetch loop started");

		loop {
			select! {
				_ = shutdown.cancelled() => {
					debug!("provider prefetch loop: shutdown signal");
					break;
				}

				result = version_rx.recv() => {
					match result {
						Ok(broadcast) => {
							// Pre-fetch this version into cache
							let version = broadcast.version;
							if let Err(e) = provider.prefetch_version(version).await {
								warn!(version = version.0, error = %e, "prefetch failed");
							}
						}
						Err(RecvError::Lagged(skipped)) => {
							warn!(skipped = skipped, "provider prefetch lagged");
						}
						Err(RecvError::Closed) => {
							debug!("provider prefetch: broadcast closed");
							break;
						}
					}
				}
			}
		}

		debug!("provider prefetch loop exited");
	}

	/// Pre-fetch a version into cache (used by background worker).
	#[instrument(name = "flow::provider::prefetch", level = "debug", skip(self), fields(
		version = version.0,
		already_cached = tracing::field::Empty,
	))]
	async fn prefetch_version(&self, version: CommitVersion) -> crate::Result<()> {
		// Check if already cached
		{
			let cache = self.cache.lock().await;
			if cache.contains_key(&version).await {
				Span::current().record("already_cached", true);
				return Ok(());
			}
		}
		Span::current().record("already_cached", false);

		// Fetch and cache - use do_fetch_and_decode directly and cache the result
		let (changes, primitives) = self.do_fetch_and_decode(version).await?;

		let arc_changes = Arc::new(changes);
		let cache = self.cache.lock().await;
		cache.put(
			version,
			CachedVersion {
				changes: arc_changes,
				primitives,
			},
		)
		.await;

		Ok(())
	}
}
