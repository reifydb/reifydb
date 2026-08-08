// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::BTreeMap,
	mem::{replace, size_of},
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::config::ConfigKey,
		cdc::{Cdc, SystemChange},
		change::{Change, Diff},
	},
	metrics::{collect::MetricsCollector, sample::MetricsSample},
};
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::{byte_size::ByteSize, reifydb_assertions, value::Value};

pub enum BacklogPull {
	Hit {
		items: Vec<Arc<Cdc>>,
		advance_to: CommitVersion,
		more: bool,
	},

	Behind,
}

struct BacklogInner {
	entries: BTreeMap<CommitVersion, (u64, Arc<Cdc>)>,
	bytes: u64,
	cover_from: Option<CommitVersion>,
}

type Waker = Box<dyn Fn() + Send + Sync>;

struct BacklogShared {
	inner: RwLock<BacklogInner>,
	limit: u64,
	anchor: AtomicU64,
	waker: RwLock<Option<Waker>>,
	armed: AtomicBool,
	published_entries: AtomicU64,
	pull_hits: AtomicU64,
	pull_behinds: AtomicU64,
	evicted_floor: AtomicU64,
	evicted_ceiling: AtomicU64,
}

#[derive(Clone)]
pub struct FlowBacklog {
	shared: Arc<BacklogShared>,
}

impl FlowBacklog {
	pub fn with_default_limit() -> Self {
		let limit = match ConfigKey::FlowBacklogMemoryLimit.default_value() {
			Value::Uint8(bytes) => ByteSize::from_bytes(bytes),
			other => panic!("FLOW_BACKLOG_MEMORY_LIMIT default must be Uint8 bytes, got {other:?}"),
		};
		Self::new(limit)
	}

	pub fn new(limit: ByteSize) -> Self {
		Self {
			shared: Arc::new(BacklogShared {
				inner: RwLock::new(BacklogInner {
					entries: BTreeMap::new(),
					bytes: 0,
					cover_from: None,
				}),
				limit: limit.as_bytes().max(1),
				anchor: AtomicU64::new(0),
				waker: RwLock::new(None),
				armed: AtomicBool::new(false),
				published_entries: AtomicU64::new(0),
				pull_hits: AtomicU64::new(0),
				pull_behinds: AtomicU64::new(0),
				evicted_floor: AtomicU64::new(0),
				evicted_ceiling: AtomicU64::new(0),
			}),
		}
	}

	pub fn limit(&self) -> ByteSize {
		ByteSize::from_bytes(self.shared.limit)
	}

	pub fn set_waker(&self, waker: impl Fn() + Send + Sync + 'static) {
		*self.shared.waker.write() = Some(Box::new(waker));
	}

	pub fn notify(&self) {
		if self.shared.armed.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok()
			&& let Some(waker) = self.shared.waker.read().as_ref()
		{
			waker();
		}
	}

	pub fn disarm(&self) {
		self.shared.armed.store(false, Ordering::Release);
	}

	pub fn publish(&self, version: CommitVersion, cdc: Option<Arc<Cdc>>) {
		let mut inner = self.shared.inner.write();
		if inner.cover_from.is_none() {
			inner.cover_from = Some(CommitVersion(version.0.saturating_sub(1)));
		}
		let Some(cdc) = cdc else {
			return;
		};
		if version <= inner.cover_from.expect("cover_from set above") {
			return;
		}
		let bytes = cdc_bytes(&cdc);
		if let Some((replaced, _)) = inner.entries.insert(version, (bytes, cdc)) {
			inner.bytes -= replaced;
		}
		inner.bytes += bytes;
		self.shared.published_entries.fetch_add(1, Ordering::Relaxed);
		self.evict_over_ceiling(&mut inner);
	}

	fn evict_over_ceiling(&self, inner: &mut BacklogInner) {
		let anchor = CommitVersion(self.shared.anchor.load(Ordering::Acquire));
		while inner.bytes > self.shared.limit {
			let Some(lowest) = inner.entries.keys().next().copied() else {
				break;
			};
			if lowest > anchor {
				break;
			}
			if let Some((evicted, _)) = inner.entries.remove(&lowest) {
				inner.bytes -= evicted;
			}
			inner.cover_from = Some(inner.cover_from.map_or(lowest, |c| c.max(lowest)));
			self.shared.evicted_ceiling.fetch_add(1, Ordering::Relaxed);
		}
	}

	pub fn pull(&self, cursor: CommitVersion, up_to: CommitVersion, budget: ByteSize) -> BacklogPull {
		if up_to <= cursor {
			return BacklogPull::Hit {
				items: Vec::new(),
				advance_to: cursor,
				more: false,
			};
		}
		let inner = self.shared.inner.read();
		let Some(cover_from) = inner.cover_from else {
			self.shared.pull_behinds.fetch_add(1, Ordering::Relaxed);
			return BacklogPull::Behind;
		};
		if cursor < cover_from {
			self.shared.pull_behinds.fetch_add(1, Ordering::Relaxed);
			return BacklogPull::Behind;
		}

		let budget = budget.as_bytes().max(1);
		let mut items: Vec<Arc<Cdc>> = Vec::new();
		let mut taken = 0u64;
		let mut truncated_at: Option<CommitVersion> = None;
		for (version, (bytes, cdc)) in inner.entries.range(next_version(cursor)..=up_to) {
			if !items.is_empty() && taken + bytes > budget {
				truncated_at = Some(*version);
				break;
			}
			taken += bytes;
			items.push(cdc.clone());
		}
		self.shared.pull_hits.fetch_add(1, Ordering::Relaxed);
		match truncated_at {
			Some(_) => BacklogPull::Hit {
				advance_to: items.last().expect("truncation implies at least one item").version,
				items,
				more: true,
			},
			None => BacklogPull::Hit {
				items,
				advance_to: up_to,
				more: false,
			},
		}
	}

	pub fn evict_below(&self, version: CommitVersion) {
		let mut inner = self.shared.inner.write();
		if inner.cover_from.is_none() {
			return;
		}
		let retained = inner.entries.split_off(&next_version(version));
		let evicted = replace(&mut inner.entries, retained);
		let count = evicted.len() as u64;
		for (bytes, _) in evicted.into_values() {
			inner.bytes -= bytes;
		}
		inner.cover_from = Some(inner.cover_from.map_or(version, |c| c.max(version)));
		self.shared.evicted_floor.fetch_add(count, Ordering::Relaxed);
	}

	pub fn set_anchor(&self, version: CommitVersion) {
		reifydb_assertions! {
			let prev = self.shared.anchor.load(Ordering::Acquire);
			assert!(
				version.0 >= prev,
				"the backlog scan anchor moved backwards ({} -> {}), so ceiling eviction could remove \
				 entries the supervisor has not scanned for DDL yet",
				prev,
				version.0
			);
		}
		self.shared.anchor.store(version.0, Ordering::Release);
	}
}

#[inline]
fn next_version(v: CommitVersion) -> CommitVersion {
	CommitVersion(v.0.saturating_add(1))
}

impl MetricsCollector for FlowBacklog {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let (bytes, count, cover_from) = {
			let inner = self.shared.inner.read();
			(inner.bytes, inner.entries.len() as u64, inner.cover_from.map(|c| c.0).unwrap_or(0))
		};
		out.push(MetricsSample::heap("flow_backlog", "bytes", ByteSize::from_bytes(bytes)));
		out.push(MetricsSample::count("flow_backlog", "entries", count));
		out.push(MetricsSample::count("flow_backlog", "cover_from", cover_from));
		out.push(MetricsSample::counter(
			"flow_backlog",
			"published_entries",
			self.shared.published_entries.load(Ordering::Relaxed),
		));
		out.push(MetricsSample::counter(
			"flow_backlog",
			"pull_hits",
			self.shared.pull_hits.load(Ordering::Relaxed),
		));
		out.push(MetricsSample::counter(
			"flow_backlog",
			"pull_behinds",
			self.shared.pull_behinds.load(Ordering::Relaxed),
		));
		out.push(MetricsSample::counter(
			"flow_backlog",
			"evicted_floor",
			self.shared.evicted_floor.load(Ordering::Relaxed),
		));
		out.push(MetricsSample::counter(
			"flow_backlog",
			"evicted_ceiling",
			self.shared.evicted_ceiling.load(Ordering::Relaxed),
		));
	}
}

pub fn cdc_bytes(cdc: &Cdc) -> u64 {
	let changes: usize = cdc.changes.iter().map(change_bytes).sum();
	let system: usize = cdc
		.system_changes
		.iter()
		.map(|change| size_of::<SystemChange>() + change.key().len() + change.value_bytes())
		.sum();
	(size_of::<Cdc>() + changes + system) as u64
}

fn change_bytes(change: &Change) -> usize {
	size_of::<Change>() + change.diffs.iter().map(diff_bytes).sum::<usize>()
}

fn diff_bytes(diff: &Diff) -> usize {
	size_of::<Diff>()
		+ match diff {
			Diff::Insert {
				post,
				..
			} => post.heap_size(),
			Diff::Update {
				pre,
				post,
				..
			} => pre.heap_size() + post.heap_size(),
			Diff::Remove {
				pre,
				..
			} => pre.heap_size(),
		}
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::AtomicUsize;

	use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
	use reifydb_value::{util::cowvec::CowVec, value::datetime::DateTime};

	use super::*;

	fn cv(n: u64) -> CommitVersion {
		CommitVersion(n)
	}

	fn cdc_with_payload(version: u64, payload: usize) -> Arc<Cdc> {
		Arc::new(Cdc::new(
			cv(version),
			DateTime::default(),
			Vec::new(),
			vec![SystemChange::Insert {
				key: EncodedKey::new(vec![0xAB; 4]),
				post: EncodedBytes(CowVec::new(vec![0u8; payload])),
			}],
		))
	}

	fn backlog(limit_bytes: u64) -> FlowBacklog {
		let b = FlowBacklog::new(ByteSize::from_bytes(limit_bytes));
		b.set_anchor(cv(u64::MAX));
		b
	}

	fn entry_bytes() -> u64 {
		cdc_bytes(&cdc_with_payload(1, 100))
	}

	#[test]
	fn pull_before_any_publish_is_behind() {
		// An empty backlog covers nothing: claiming coverage would let a flow with an old
		// checkpoint skip its whole catch-up range as if it carried no CDC.
		let b = backlog(u64::MAX);
		assert!(matches!(b.pull(cv(0), cv(10), ByteSize::from_mib(1)), BacklogPull::Behind));
	}

	#[test]
	fn coverage_starts_just_below_the_first_published_version() {
		// The first publish establishes the floor: from that version on the backlog is authoritative,
		// anything earlier lives only on disk and must be sent to the loader.
		let b = backlog(u64::MAX);
		b.publish(cv(100), Some(cdc_with_payload(100, 10)));
		match b.pull(cv(99), cv(100), ByteSize::from_mib(1)) {
			BacklogPull::Hit {
				items,
				advance_to,
				more,
			} => {
				assert_eq!(items.len(), 1);
				assert_eq!(advance_to, cv(100));
				assert!(!more);
			}
			BacklogPull::Behind => panic!("cursor at cover_from must be served"),
		}
		assert!(matches!(b.pull(cv(98), cv(100), ByteSize::from_mib(1)), BacklogPull::Behind));
	}

	#[test]
	fn irrelevant_versions_extend_coverage_without_entries() {
		// Versions carrying nothing a flow cares about must still extend coverage, or crossing
		// them would cost a disk trip for no data.
		let b = backlog(u64::MAX);
		b.publish(cv(5), None);
		match b.pull(cv(4), cv(9), ByteSize::from_mib(1)) {
			BacklogPull::Hit {
				items,
				advance_to,
				more,
			} => {
				assert!(items.is_empty());
				assert_eq!(advance_to, cv(9), "an empty pull must advance to the caller's bound");
				assert!(!more);
			}
			BacklogPull::Behind => panic!("published coverage must serve the empty range"),
		}
	}

	#[test]
	fn pull_up_to_at_or_below_cursor_is_an_empty_hit() {
		let b = backlog(u64::MAX);
		b.publish(cv(5), Some(cdc_with_payload(5, 10)));
		match b.pull(cv(5), cv(5), ByteSize::from_mib(1)) {
			BacklogPull::Hit {
				items,
				advance_to,
				..
			} => {
				assert!(items.is_empty());
				assert_eq!(advance_to, cv(5));
			}
			BacklogPull::Behind => panic!("nothing to pull is not Behind"),
		}
	}

	#[test]
	fn budget_truncation_reports_more_and_advances_only_to_the_last_taken() {
		// advance_to on a truncated pull must be the last item actually handed out; advancing
		// to the bound would checkpoint past entries the flow never applied, losing them.
		let b = backlog(u64::MAX);
		for v in 1..=4 {
			b.publish(cv(v), Some(cdc_with_payload(v, 100)));
		}
		let two = entry_bytes() * 2;
		match b.pull(cv(0), cv(4), ByteSize::from_bytes(two)) {
			BacklogPull::Hit {
				items,
				advance_to,
				more,
			} => {
				assert_eq!(items.len(), 2);
				assert_eq!(advance_to, cv(2));
				assert!(more, "a truncated pull must tell the caller to come back");
			}
			BacklogPull::Behind => panic!("expected Hit"),
		}
	}

	#[test]
	fn a_single_oversized_entry_is_still_served() {
		// The budget bounds batching, not progress: an entry larger than the whole budget must
		// still be handed out alone, or the flow would spin forever on an empty pull.
		let b = backlog(u64::MAX);
		b.publish(cv(1), Some(cdc_with_payload(1, 4096)));
		match b.pull(cv(0), cv(1), ByteSize::from_bytes(1)) {
			BacklogPull::Hit {
				items,
				advance_to,
				more,
			} => {
				assert_eq!(items.len(), 1);
				assert_eq!(advance_to, cv(1));
				assert!(!more);
			}
			BacklogPull::Behind => panic!("expected Hit"),
		}
	}

	#[test]
	fn evict_below_raises_the_floor_and_later_pulls_go_behind() {
		let b = backlog(u64::MAX);
		for v in 1..=4 {
			b.publish(cv(v), Some(cdc_with_payload(v, 100)));
		}
		b.evict_below(cv(2));
		assert!(
			matches!(b.pull(cv(1), cv(4), ByteSize::from_mib(1)), BacklogPull::Behind),
			"a cursor below the raised floor must be sent to the loader"
		);
		match b.pull(cv(2), cv(4), ByteSize::from_mib(1)) {
			BacklogPull::Hit {
				items,
				..
			} => assert_eq!(items.len(), 2),
			BacklogPull::Behind => panic!("entries above the floor must survive evict_below"),
		}
	}

	#[test]
	fn ceiling_eviction_drops_lowest_versions_first_and_raises_the_floor() {
		// The deepest laggard is the one who pays disk again: the ceiling evicts from the
		// bottom so the near-frontier window every healthy flow reads stays resident.
		let one = entry_bytes();
		let b = backlog(one * 2);
		for v in 1..=3 {
			b.publish(cv(v), Some(cdc_with_payload(v, 100)));
		}
		assert!(matches!(b.pull(cv(0), cv(3), ByteSize::from_mib(1)), BacklogPull::Behind));
		match b.pull(cv(1), cv(3), ByteSize::from_mib(1)) {
			BacklogPull::Hit {
				items,
				..
			} => assert_eq!(items.len(), 2, "the two newest entries must survive"),
			BacklogPull::Behind => panic!("expected Hit above the evicted floor"),
		}
	}

	#[test]
	fn ceiling_eviction_never_crosses_the_scan_anchor() {
		// Entries above the anchor have not been scanned by the supervisor for flow DDL yet;
		// evicting them would let flow creations or deletions vanish without being processed.
		// The ceiling is soft against the anchor: bytes exceed the limit until the anchor moves.
		let one = entry_bytes();
		let b = FlowBacklog::new(ByteSize::from_bytes(one));
		b.set_anchor(cv(1));
		for v in 1..=3 {
			b.publish(cv(v), Some(cdc_with_payload(v, 100)));
		}
		match b.pull(cv(1), cv(3), ByteSize::from_mib(1)) {
			BacklogPull::Hit {
				items,
				..
			} => assert_eq!(items.len(), 2, "unscanned entries must survive the ceiling"),
			BacklogPull::Behind => panic!("entries above the anchor must not be evicted"),
		}
		b.set_anchor(cv(3));
		b.publish(cv(4), Some(cdc_with_payload(4, 100)));
		assert!(
			matches!(b.pull(cv(1), cv(4), ByteSize::from_mib(1)), BacklogPull::Behind),
			"once the anchor passes them, over-ceiling entries must be evicted lowest-first"
		);
	}

	#[test]
	fn out_of_order_publish_below_the_floor_is_ignored() {
		// The producer can process commits out of order; a version arriving below the established
		// floor cannot extend coverage downward, and a stray entry there would contradict Behind.
		let b = backlog(u64::MAX);
		b.publish(cv(101), Some(cdc_with_payload(101, 10)));
		b.publish(cv(99), Some(cdc_with_payload(99, 10)));
		assert!(matches!(b.pull(cv(98), cv(101), ByteSize::from_mib(1)), BacklogPull::Behind));
		match b.pull(cv(100), cv(101), ByteSize::from_mib(1)) {
			BacklogPull::Hit {
				items,
				..
			} => assert_eq!(items.len(), 1),
			BacklogPull::Behind => panic!("expected Hit"),
		}
	}

	#[test]
	fn notify_fires_once_until_disarmed() {
		// A burst of publishes must coalesce into one supervisor wake; without the re-arm on
		// disarm, a supervisor that scanned everything would sleep through all later CDC.
		let fired = Arc::new(AtomicUsize::new(0));
		let b = backlog(u64::MAX);
		let counter = fired.clone();
		b.set_waker(move || {
			counter.fetch_add(1, Ordering::SeqCst);
		});
		b.notify();
		b.notify();
		b.notify();
		assert_eq!(fired.load(Ordering::SeqCst), 1, "repeat notifies while armed must coalesce");
		b.disarm();
		b.notify();
		assert_eq!(fired.load(Ordering::SeqCst), 2, "a disarmed backlog must wake again");
	}

	#[test]
	fn byte_accounting_balances_across_publish_replace_and_eviction() {
		// The ceiling compares against this tally, so drift here breaks eviction itself, not just
		// the reported metric.
		let one = entry_bytes();
		let b = backlog(u64::MAX);
		b.publish(cv(1), Some(cdc_with_payload(1, 100)));
		b.publish(cv(2), Some(cdc_with_payload(2, 100)));
		b.publish(cv(2), Some(cdc_with_payload(2, 300)));
		let mut out = Vec::new();
		b.collect(&mut out);
		let bytes = out
			.iter()
			.find(|s| s.scope == "flow_backlog" && s.metric == "bytes")
			.map(|s| s.reading.as_f64())
			.expect("bytes sample");
		assert_eq!(bytes, (one + one + 200) as f64, "replacing an entry must swap its tally, not add");

		b.evict_below(cv(2));
		let mut out = Vec::new();
		b.collect(&mut out);
		let bytes = out
			.iter()
			.find(|s| s.scope == "flow_backlog" && s.metric == "bytes")
			.map(|s| s.reading.as_f64())
			.expect("bytes sample");
		assert_eq!(bytes, 0.0, "evicting every entry must zero the tally");
	}
}
