// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::Bound, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcBatch},
};
use reifydb_value::value::datetime::DateTime;
use tracing::instrument;

use super::{
	CdcStorage, CdcStorageResult, DropBeforeResult, normalize_range_inclusive,
	recent_cache::{RangeLookup, RecentCdcCache},
};

#[derive(Clone)]
pub struct CachedCdcStorage<S: CdcStorage> {
	inner: S,
	cache: RecentCdcCache,
}

impl<S: CdcStorage> CachedCdcStorage<S> {
	pub fn new(inner: S, capacity: usize) -> Self {
		Self {
			inner,
			cache: RecentCdcCache::new(capacity),
		}
	}

	pub fn inner(&self) -> &S {
		&self.inner
	}

	#[instrument(name = "store::cdc::cached::read_hit", level = "debug", skip_all)]
	fn read_hit(&self, cdc: Arc<Cdc>) -> CdcStorageResult<Option<Cdc>> {
		Ok(Some((*cdc).clone()))
	}

	#[instrument(name = "store::cdc::cached::read_miss", level = "debug", skip_all)]
	fn read_miss(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		self.inner.read(version)
	}

	#[instrument(name = "store::cdc::cached::range_hit", level = "debug", skip_all)]
	fn range_hit(&self, items: Vec<Cdc>, has_more: bool) -> CdcStorageResult<CdcBatch> {
		Ok(CdcBatch {
			items,
			has_more,
		})
	}

	#[instrument(name = "store::cdc::cached::range_partial", level = "debug", skip_all)]
	fn range_partial(
		&self,
		lo_inc: CommitVersion,
		floor: CommitVersion,
		batch_size: u64,
		limit: usize,
		tail: Vec<Cdc>,
		tail_has_more: bool,
	) -> CdcStorageResult<CdcBatch> {
		let head = self.inner.read_range(Bound::Included(lo_inc), Bound::Excluded(floor), batch_size)?;
		let mut items = head.items;
		let mut has_more = head.has_more;
		if !has_more && items.len() < limit {
			let remaining = limit - items.len();
			let take = tail.len().min(remaining);
			has_more = tail_has_more || tail.len() > take;
			items.extend(tail.into_iter().take(take));
		}
		Ok(CdcBatch {
			items,
			has_more,
		})
	}

	#[instrument(name = "store::cdc::cached::range_miss", level = "debug", skip_all)]
	fn range_miss(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		self.inner.read_range(start, end, batch_size)
	}
}

impl<S: CdcStorage> CdcStorage for CachedCdcStorage<S> {
	fn write(&self, cdc: &Cdc) -> CdcStorageResult<()> {
		self.inner.write(cdc)?;
		self.cache.insert(cdc);
		Ok(())
	}

	fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
		if let Some(cdc) = self.cache.get(version) {
			return self.read_hit(cdc);
		}
		self.read_miss(version)
	}

	fn read_range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> CdcStorageResult<CdcBatch> {
		let Some((lo_inc, hi_inc)) = normalize_range_inclusive(start, end) else {
			return self.range_miss(start, end, batch_size);
		};
		let limit = batch_size as usize;

		match self.cache.lookup_range(lo_inc, hi_inc, limit) {
			RangeLookup::Hit {
				items,
				has_more,
			} => self.range_hit(items, has_more),
			RangeLookup::Overlap {
				floor,
				tail,
				tail_has_more,
			} => self.range_partial(lo_inc, floor, batch_size, limit, tail, tail_has_more),
			RangeLookup::Miss => self.range_miss(start, end, batch_size),
		}
	}

	fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
		self.inner.count(version)
	}

	fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		self.inner.min_version()
	}

	fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
		self.inner.max_version()
	}

	fn drop_before(&self, version: CommitVersion, limit: usize) -> CdcStorageResult<DropBeforeResult> {
		self.inner.drop_before(version, limit)
	}

	fn find_ttl_cutoff(&self, cutoff: DateTime) -> CdcStorageResult<Option<CommitVersion>> {
		self.inner.find_ttl_cutoff(cutoff)
	}
}

#[cfg(test)]
mod tests {
	use std::{collections::Bound, sync::Arc};

	use reifydb_core::{common::CommitVersion, interface::cdc::Cdc};
	use reifydb_runtime::sync::mutex::Mutex;
	use reifydb_value::value::datetime::DateTime;

	use super::*;
	use crate::storage::memory::MemoryCdcStorage;

	fn cv(n: u64) -> CommitVersion {
		CommitVersion(n)
	}

	fn cdc(version: u64) -> Cdc {
		Cdc::new(cv(version), DateTime::default(), Vec::new(), Vec::new())
	}

	#[test]
	fn write_is_persisted_to_inner_and_served_from_cache() {
		let cached = CachedCdcStorage::new(MemoryCdcStorage::new(), 16);
		cached.write(&cdc(1)).unwrap();
		// inner has it durably
		assert!(cached.inner().read(cv(1)).unwrap().is_some());
		// and the cache serves the read
		assert_eq!(cached.read(cv(1)).unwrap().unwrap().version, cv(1));
	}

	#[test]
	fn read_range_served_from_cache_when_covered() {
		let cached = CachedCdcStorage::new(MemoryCdcStorage::new(), 16);
		for v in 1..=5 {
			cached.write(&cdc(v)).unwrap();
		}
		let batch = cached.read_range(Bound::Excluded(cv(1)), Bound::Included(cv(4)), 100).unwrap();
		assert_eq!(batch.items.iter().map(|c| c.version).collect::<Vec<_>>(), vec![cv(2), cv(3), cv(4)]);
		assert!(!batch.has_more);
	}

	#[test]
	fn read_range_falls_back_to_inner_when_entirely_below_cache_window() {
		// Capacity 2 keeps only versions {4,5}; a request ending before that window has no
		// overlap with the cache at all, so the decorator must serve it purely from the backend.
		let inner = MemoryCdcStorage::new();
		let cached = CachedCdcStorage::new(inner, 2);
		for v in 1..=5 {
			cached.write(&cdc(v)).unwrap();
		}
		let batch = cached.read_range(Bound::Included(cv(1)), Bound::Included(cv(3)), 100).unwrap();
		assert_eq!(batch.items.iter().map(|c| c.version).collect::<Vec<_>>(), vec![cv(1), cv(2), cv(3)]);
	}

	#[test]
	fn read_range_merges_inner_head_with_cached_tail_when_partially_covered() {
		// Capacity 2 keeps only versions {4,5}; a request for [1,5] straddles the cache window,
		// so the decorator must stitch the backend-served head onto the cache-served tail rather
		// than discarding the cache and re-reading everything from the backend.
		let recording = RecordingCdcStorage::new(MemoryCdcStorage::new());
		let cached = CachedCdcStorage::new(recording.clone(), 2);
		for v in 1..=5 {
			cached.write(&cdc(v)).unwrap();
		}
		recording.calls.lock().clear();

		let batch = cached.read_range(Bound::Included(cv(1)), Bound::Included(cv(5)), 100).unwrap();
		assert_eq!(
			batch.items.iter().map(|c| c.version).collect::<Vec<_>>(),
			vec![cv(1), cv(2), cv(3), cv(4), cv(5)]
		);
		assert!(!batch.has_more);

		// The backend must only have been asked for the uncovered head (versions below the
		// cache's floor of 4), never for the full [1,5] range the cache already partly serves.
		let calls = recording.calls.lock();
		assert_eq!(calls.len(), 1, "expected exactly one backend read_range call, got {calls:?}");
		assert_eq!(calls[0], (Bound::Included(cv(1)), Bound::Excluded(cv(4))));
	}

	#[test]
	fn read_range_merge_respects_batch_size_across_head_and_tail() {
		let inner = MemoryCdcStorage::new();
		let cached = CachedCdcStorage::new(inner, 2);
		for v in 1..=5 {
			cached.write(&cdc(v)).unwrap();
		}
		// head [1,3] has 3 entries, cache tail [4,5] has 2; a batch_size of 4 must take all of
		// the head plus one from the tail, and report has_more since the tail was truncated.
		let batch = cached.read_range(Bound::Included(cv(1)), Bound::Included(cv(5)), 4).unwrap();
		assert_eq!(batch.items.iter().map(|c| c.version).collect::<Vec<_>>(), vec![cv(1), cv(2), cv(3), cv(4)]);
		assert!(batch.has_more);
	}

	struct SpanNameCaptureLayer {
		spans: Arc<Mutex<Vec<String>>>,
	}

	impl<S> tracing_subscriber::Layer<S> for SpanNameCaptureLayer
	where
		S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
	{
		fn on_new_span(
			&self,
			attrs: &tracing::span::Attributes<'_>,
			_id: &tracing::span::Id,
			_ctx: tracing_subscriber::layer::Context<'_, S>,
		) {
			self.spans.lock().push(attrs.metadata().name().to_string());
		}
	}

	fn capture_spans(f: impl FnOnce()) -> Vec<String> {
		use tracing_subscriber::layer::SubscriberExt;

		let spans = Arc::new(Mutex::new(Vec::new()));
		let layer = SpanNameCaptureLayer {
			spans: spans.clone(),
		};
		let subscriber = tracing_subscriber::Registry::default().with(layer);
		tracing::subscriber::with_default(subscriber, f);
		spans.lock().clone()
	}

	#[test]
	fn read_hit_emits_read_hit_span() {
		let cached = CachedCdcStorage::new(MemoryCdcStorage::new(), 16);
		cached.write(&cdc(1)).unwrap();

		let spans = capture_spans(|| {
			cached.read(cv(1)).unwrap();
		});
		assert_eq!(spans, vec!["store::cdc::cached::read_hit"]);
	}

	#[test]
	fn read_miss_emits_read_miss_span() {
		// Capacity 2 keeps only versions {4,5}; version 1 fell out of the cache.
		let cached = CachedCdcStorage::new(MemoryCdcStorage::new(), 2);
		for v in 1..=5 {
			cached.write(&cdc(v)).unwrap();
		}

		let spans = capture_spans(|| {
			cached.read(cv(1)).unwrap();
		});
		assert_eq!(spans, vec!["store::cdc::cached::read_miss"]);
	}

	#[test]
	fn range_hit_emits_range_hit_span() {
		let cached = CachedCdcStorage::new(MemoryCdcStorage::new(), 16);
		for v in 1..=5 {
			cached.write(&cdc(v)).unwrap();
		}

		let spans = capture_spans(|| {
			cached.read_range(Bound::Excluded(cv(1)), Bound::Included(cv(4)), 100).unwrap();
		});
		assert_eq!(spans, vec!["store::cdc::cached::range_hit"]);
	}

	#[test]
	fn range_partial_emits_range_partial_span() {
		let cached = CachedCdcStorage::new(MemoryCdcStorage::new(), 2);
		for v in 1..=5 {
			cached.write(&cdc(v)).unwrap();
		}

		let spans = capture_spans(|| {
			cached.read_range(Bound::Included(cv(1)), Bound::Included(cv(5)), 100).unwrap();
		});
		assert_eq!(spans, vec!["store::cdc::cached::range_partial"]);
	}

	#[test]
	fn range_miss_emits_range_miss_span() {
		let cached = CachedCdcStorage::new(MemoryCdcStorage::new(), 2);
		for v in 1..=5 {
			cached.write(&cdc(v)).unwrap();
		}

		let spans = capture_spans(|| {
			cached.read_range(Bound::Included(cv(1)), Bound::Included(cv(3)), 100).unwrap();
		});
		assert_eq!(spans, vec!["store::cdc::cached::range_miss"]);
	}

	type RecordedRange = (Bound<CommitVersion>, Bound<CommitVersion>);

	#[derive(Clone)]
	struct RecordingCdcStorage<S: CdcStorage> {
		inner: S,
		calls: Arc<Mutex<Vec<RecordedRange>>>,
	}

	impl<S: CdcStorage> RecordingCdcStorage<S> {
		fn new(inner: S) -> Self {
			Self {
				inner,
				calls: Arc::new(Mutex::new(Vec::new())),
			}
		}
	}

	impl<S: CdcStorage> CdcStorage for RecordingCdcStorage<S> {
		fn write(&self, cdc: &Cdc) -> CdcStorageResult<()> {
			self.inner.write(cdc)
		}

		fn read(&self, version: CommitVersion) -> CdcStorageResult<Option<Cdc>> {
			self.inner.read(version)
		}

		fn read_range(
			&self,
			start: Bound<CommitVersion>,
			end: Bound<CommitVersion>,
			batch_size: u64,
		) -> CdcStorageResult<CdcBatch> {
			self.calls.lock().push((start, end));
			self.inner.read_range(start, end, batch_size)
		}

		fn count(&self, version: CommitVersion) -> CdcStorageResult<usize> {
			self.inner.count(version)
		}

		fn min_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
			self.inner.min_version()
		}

		fn max_version(&self) -> CdcStorageResult<Option<CommitVersion>> {
			self.inner.max_version()
		}

		fn drop_before(&self, version: CommitVersion, limit: usize) -> CdcStorageResult<DropBeforeResult> {
			self.inner.drop_before(version, limit)
		}

		fn find_ttl_cutoff(&self, cutoff: DateTime) -> CdcStorageResult<Option<CommitVersion>> {
			self.inner.find_ttl_cutoff(cutoff)
		}
	}
}
