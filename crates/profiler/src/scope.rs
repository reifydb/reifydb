// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Per-request profiling scope. A `ProfileScope::start` opens a scope and returns a `ScopeHandle`; the layer
//! discovers the scope via two paths: a `tokio::task_local!` (`ACTIVE_SCOPE`) carrying the `ScopeId` for spans
//! created on the same task, or via an ancestor span's extension stamped by the layer on its first sighting. State
//! lives behind `Arc<ScopeState>` so spans on any thread that reach the same scope contribute to the same buffer.
//! `ScopeHandle::finish` drains the buffer, builds the `ProfileSummary`, and dispatches it to the configured sink.

use std::{
	future::Future,
	mem,
	sync::{
		Arc, OnceLock,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	time::{Instant, SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::task_local;

use crate::{
	intern::DimInterner,
	record::MinimalSpanRecord,
	sink::{NoopSink, ProfileSink},
	summary::ProfileSummary,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScopeId(pub u64);

static NEXT_SCOPE_ID: AtomicU64 = AtomicU64::new(1);

fn next_scope_id() -> ScopeId {
	ScopeId(NEXT_SCOPE_ID.fetch_add(1, Ordering::Relaxed))
}

pub struct ScopeState {
	pub id: ScopeId,
	pub name: &'static str,
	pub started_at: Instant,
	pub started_at_nanos: u128,
	pub records: Mutex<Vec<MinimalSpanRecord>>,
	pub batch_threshold: usize,
	pub closed: AtomicBool,
	pub sink: Arc<dyn ProfileSink>,
	pub interner: OnceLock<Arc<DimInterner>>,
}

impl ScopeState {
	pub fn push(&self, rec: MinimalSpanRecord) {
		if self.closed.load(Ordering::Acquire) {
			return;
		}
		let mut guard = self.records.lock();
		guard.push(rec);
		if self.batch_threshold > 0 && guard.len() >= self.batch_threshold {
			let drained: Vec<MinimalSpanRecord> = mem::take(&mut *guard);
			drop(guard);
			let elapsed_us = self.started_at.elapsed().as_micros() as u64;
			let summary = ProfileSummary::from_records(
				self.id,
				self.name,
				self.started_at_nanos,
				elapsed_us,
				drained,
				self.interner.get().cloned(),
			);
			self.sink.on_scope_batch(&summary);
		}
	}

	pub fn attach_interner(&self, interner: Arc<DimInterner>) {
		let _ = self.interner.set(interner);
	}
}

pub(crate) struct ScopeRegistry {
	scopes: DashMap<ScopeId, Arc<ScopeState>>,
}

impl ScopeRegistry {
	fn new() -> Self {
		Self {
			scopes: DashMap::new(),
		}
	}

	pub(crate) fn insert(&self, state: Arc<ScopeState>) {
		self.scopes.insert(state.id, state);
	}

	pub(crate) fn get(&self, id: ScopeId) -> Option<Arc<ScopeState>> {
		self.scopes.get(&id).map(|r| Arc::clone(r.value()))
	}

	pub(crate) fn remove(&self, id: ScopeId) -> Option<Arc<ScopeState>> {
		self.scopes.remove(&id).map(|(_, v)| v)
	}
}

impl Default for ScopeRegistry {
	fn default() -> Self {
		Self::new()
	}
}

pub(crate) static REGISTRY: Lazy<ScopeRegistry> = Lazy::new(ScopeRegistry::default);

task_local! {
	pub(crate) static ACTIVE_SCOPE: ScopeId;
}

pub struct ProfileScope;

pub struct ScopeHandle {
	state: Arc<ScopeState>,
}

const DEFAULT_BATCH_THRESHOLD: usize = 256;

impl ProfileScope {
	pub fn start(name: &'static str) -> ScopeHandle {
		Self::start_with_sink(name, Arc::new(NoopSink))
	}

	pub fn start_with_sink(name: &'static str, sink: Arc<dyn ProfileSink>) -> ScopeHandle {
		let state = build_scope_state(name, sink);
		REGISTRY.insert(Arc::clone(&state));
		ScopeHandle {
			state,
		}
	}

	pub fn ambient(name: &'static str, sink: Arc<dyn ProfileSink>) -> Arc<ScopeState> {
		let state = build_scope_state(name, sink);
		REGISTRY.insert(Arc::clone(&state));
		state
	}
}

fn build_scope_state(name: &'static str, sink: Arc<dyn ProfileSink>) -> Arc<ScopeState> {
	let id = next_scope_id();
	let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
	Arc::new(ScopeState {
		id,
		name,
		started_at: Instant::now(),
		started_at_nanos: nanos,
		records: Mutex::new(Vec::with_capacity(DEFAULT_BATCH_THRESHOLD)),
		batch_threshold: DEFAULT_BATCH_THRESHOLD,
		closed: AtomicBool::new(false),
		sink,
		interner: OnceLock::new(),
	})
}

impl ScopeHandle {
	pub fn id(&self) -> ScopeId {
		self.state.id
	}

	pub fn name(&self) -> &'static str {
		self.state.name
	}

	pub async fn run<F, R>(&self, fut: F) -> R
	where
		F: Future<Output = R>,
	{
		ACTIVE_SCOPE.scope(self.state.id, fut).await
	}

	pub fn run_sync<F, R>(&self, f: F) -> R
	where
		F: FnOnce() -> R,
	{
		ACTIVE_SCOPE.sync_scope(self.state.id, f)
	}

	pub fn finish(self) -> ProfileSummary {
		self.state.closed.store(true, Ordering::Release);
		REGISTRY.remove(self.state.id);
		let records: Vec<MinimalSpanRecord> = mem::take(&mut *self.state.records.lock());
		let elapsed_us = self.state.started_at.elapsed().as_micros() as u64;
		let summary = ProfileSummary::from_records(
			self.state.id,
			self.state.name,
			self.state.started_at_nanos,
			elapsed_us,
			records,
			self.state.interner.get().cloned(),
		);
		self.state.sink.on_scope_closed(&summary);
		summary
	}
}

pub fn active_scope() -> Option<ScopeId> {
	ACTIVE_SCOPE.try_with(|id| *id).ok()
}

pub fn lookup_scope(id: ScopeId) -> Option<Arc<ScopeState>> {
	REGISTRY.get(id)
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicUsize, Ordering};

	use super::*;
	use crate::{category::ProfileCategory, record::MinimalSpanRecord};

	#[test]
	fn scope_id_monotonic() {
		let a = next_scope_id();
		let b = next_scope_id();
		assert!(b.0 > a.0);
	}

	#[test]
	fn finish_drains_records_and_marks_closed() {
		let handle = ProfileScope::start("test.scope");
		let id = handle.id();
		let state = lookup_scope(id).expect("scope registered");
		state.push(MinimalSpanRecord::new(ProfileCategory::Query, 1, 100));
		state.push(MinimalSpanRecord::new(ProfileCategory::Query, 2, 200));

		let summary = handle.finish();
		assert_eq!(summary.records.len(), 2);
		assert_eq!(summary.category(ProfileCategory::Query).calls, 2);
		assert!(lookup_scope(id).is_none());
	}

	#[test]
	fn push_after_finish_is_ignored() {
		let handle = ProfileScope::start("test.scope");
		let state = lookup_scope(handle.id()).unwrap();
		let _ = handle.finish();
		state.push(MinimalSpanRecord::new(ProfileCategory::Storage, 1, 50));
		assert!(state.records.lock().is_empty());
	}

	#[test]
	fn batch_threshold_drains_via_sink() {
		#[derive(Default)]
		struct CountingSink {
			batches: AtomicUsize,
		}
		impl ProfileSink for CountingSink {
			fn on_scope_closed(&self, _s: &ProfileSummary) {}
			fn on_scope_batch(&self, _s: &ProfileSummary) {
				self.batches.fetch_add(1, Ordering::Relaxed);
			}
		}
		let sink: Arc<CountingSink> = Arc::new(CountingSink::default());
		let handle = ProfileScope::start_with_sink("test.scope", sink.clone());
		let state = lookup_scope(handle.id()).unwrap();
		for i in 0..DEFAULT_BATCH_THRESHOLD {
			state.push(MinimalSpanRecord::new(ProfileCategory::Flow, i as u64, 10));
		}
		assert_eq!(sink.batches.load(Ordering::Relaxed), 1);
		assert!(state.records.lock().is_empty());
		let _ = handle.finish();
	}

	#[tokio::test]
	async fn run_sets_active_scope() {
		let handle = ProfileScope::start("async.scope");
		let id = handle.id();
		let observed: ScopeId = handle.run(async move { active_scope().unwrap() }).await;
		assert_eq!(observed, id);
		let _ = handle.finish();
	}

	#[test]
	fn run_sync_sets_active_scope() {
		let handle = ProfileScope::start("sync.scope");
		let id = handle.id();
		let observed = handle.run_sync(active_scope);
		assert_eq!(observed, Some(id));
		let _ = handle.finish();
	}
}
