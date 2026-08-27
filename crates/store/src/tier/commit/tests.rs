// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::metrics::collect::MetricsCollector;
use reifydb_runtime::sync::{condvar::Condvar, mutex::Mutex};
use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

use crate::tier::commit::{
	CommitConfig, CommitDomain, CommitTier, CommitWaker,
	domain::{FLOOR, FloorDomain as F, TestDomain as D, TestState, footprint},
};

fn config(limit: u64) -> CommitConfig {
	CommitConfig {
		budget: Some(ByteSize::from_bytes(limit)),
		interval: Duration::from_seconds_const(5),
	}
}

fn tier(limit: u64) -> CommitTier<D> {
	CommitTier::<D>::new(config(limit), TestState::new).expect("a tier with a byte budget must be constructed")
}

fn floor_tier(limit: u64) -> CommitTier<F> {
	CommitTier::<F>::new(config(limit), TestState::new).expect("a tier with a byte budget must be constructed")
}

fn roomy() -> CommitTier<D> {
	tier(ByteSize::from_mib(1).as_bytes())
}

fn key(name: &str) -> EncodedKey {
	EncodedKey::new(name.as_bytes())
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn cost(name: &str, body: Option<&str>) -> ByteSize {
	footprint(&key(name), &body.map(row))
}

struct Gate {
	open: Mutex<bool>,
	signal: Condvar,
}

impl Gate {
	fn new() -> Arc<Self> {
		Arc::new(Self {
			open: Mutex::new(false),
			signal: Condvar::new(),
		})
	}

	fn open(&self) {
		let mut open = self.open.lock();
		*open = true;
		self.signal.notify_all();
	}

	fn wait(&self) {
		let mut open = self.open.lock();
		while !*open {
			self.signal.wait(&mut open);
		}
	}
}

#[derive(Default)]
struct CountingWaker {
	wakes: AtomicU64,
}

impl CommitWaker for CountingWaker {
	fn wake(&self) {
		self.wakes.fetch_add(1, Ordering::SeqCst);
	}
}

#[test]
fn a_tier_without_a_budget_is_not_built() {
	let built = CommitTier::<D>::new(
		CommitConfig {
			budget: None,
			interval: Duration::from_seconds_const(5),
		},
		TestState::new,
	);
	assert!(built.is_none(), "a tier with no configured budget must refuse to exist");
}

#[test]
fn a_new_key_charges_its_overhead_key_and_row_once() {
	let tier = roomy();
	tier.state().record(0, key("alpha"), Some(row("value")), 1);

	assert_eq!(tier.resident_bytes(), cost("alpha", Some("value")), "insert must charge the whole footprint");
	assert_eq!(tier.state().live_bytes(), tier.state().census_bytes(), "the incremental counter must match a scan");
}

#[test]
fn a_collapse_swaps_only_the_row_charge() {
	let tier = roomy();
	tier.state().record(0, key("alpha"), Some(row("aa")), 1);
	let after_first = tier.resident_bytes();
	tier.state().record(0, key("alpha"), Some(row("bbbbbbbb")), 2);

	assert_eq!(tier.state().live_entries(0), 1, "a collapse must leave one entry, not two");
	assert_eq!(tier.resident_bytes(), cost("alpha", Some("bbbbbbbb")), "the charge must be the new footprint");
	assert_eq!(
		tier.resident_bytes().as_bytes() - after_first.as_bytes(),
		6,
		"only the six extra row bytes may be charged, never the key or the overhead again"
	);
	assert_eq!(tier.state().live_bytes(), tier.state().census_bytes(), "the incremental counter must match a scan");
}

#[test]
fn a_collapse_to_a_tombstone_keeps_the_key_charge() {
	let tier = roomy();
	tier.state().record(0, key("alpha"), Some(row("value")), 1);
	tier.state().remove(0, key("alpha"), 2);

	assert_eq!(tier.state().live_entries(0), 1, "a tombstone stays resident until it is flushed");
	assert_eq!(tier.resident_bytes(), cost("alpha", None), "the key and overhead charge must survive the removal");
	assert!(tier.resident_bytes() > ByteSize::ZERO, "a tombstone is not free");
	assert_eq!(tier.state().live_bytes(), tier.state().census_bytes(), "the incremental counter must match a scan");
}

#[test]
fn every_dropped_entry_refunds_its_whole_footprint() {
	let tier = roomy();
	for name in ["a", "bb", "ccc", "dddd"] {
		tier.state().record(0, key(name), Some(row(name)), 1);
	}
	let before = tier.resident_bytes();
	let refunded = cost("bb", Some("bb")).saturating_add(cost("dddd", Some("dddd")));

	tier.state().drop_where(0, |candidate| candidate.as_slice().len() % 2 == 1);

	assert_eq!(tier.state().live_entries(0), 2, "the predicate keeps the odd-length keys");
	assert_eq!(tier.resident_bytes(), before.saturating_sub(refunded), "each dropped entry must refund in full");
	assert_eq!(tier.state().live_bytes(), tier.state().census_bytes(), "the incremental counter must match a scan");
}

#[test]
fn a_split_moves_the_charge_exactly() {
	let tier = roomy();
	for name in ["a", "b", "c", "d"] {
		tier.state().record(0, key(name), Some(row("payload")), 1);
	}
	let total = tier.resident_bytes();
	let one = cost("a", Some("payload"));

	let gate = Gate::new();
	let parked = gate.clone();
	let observed = Arc::new(Mutex::new((ByteSize::ZERO, ByteSize::ZERO, ByteSize::ZERO)));
	let sink = observed.clone();
	tier.state().set_hook(Box::new(move |state| {
		*sink.lock() = (state.live_bytes(), state.in_flight_bytes(), state.budget_used());
		parked.open();
	}));

	tier.flush_slice(one * 2);
	gate.wait();

	let (live, in_flight, used) = *observed.lock();
	assert_eq!(in_flight, (one * 2), "the batch must carry exactly the bytes it took");
	assert_eq!(live, total.saturating_sub(in_flight), "the source must lose exactly what the batch took");
	assert_eq!(live.saturating_add(in_flight), total, "source plus taken must equal the original");
	assert_eq!(used, total, "the budget must still see every byte the split moved");
}

#[test]
fn an_entry_wider_than_the_slice_budget_is_taken_alone() {
	let tier = roomy();
	tier.state().record(0, key("wide"), Some(row("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")), 1);

	let outcome = tier.flush_slice(ByteSize::from_bytes(1));

	assert_eq!(outcome.persisted, 1, "one oversized entry must still make progress");
	assert_eq!(tier.state().live_entries(0), 0, "the oversized entry must leave the resident set");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "its charge must be released once it is durable");
}

#[test]
fn bytes_are_released_at_settle_not_at_select() {
	let tier = roomy();
	for name in ["a", "b", "c"] {
		tier.state().record(0, key(name), Some(row("payload")), 1);
	}
	let total = tier.resident_bytes();

	let seen = Arc::new(Mutex::new((ByteSize::ZERO, ByteSize::ZERO, ByteSize::ZERO)));
	let sink = seen.clone();
	tier.state().set_hook(Box::new(move |state| {
		*sink.lock() = (state.budget_used(), state.live_bytes(), state.in_flight_bytes());
	}));

	tier.flush_pending();

	let (used_in_flight, live_in_flight, carried) = *seen.lock();
	assert_eq!(used_in_flight, total, "the budget must still hold every byte while the batch is out");
	assert_eq!(live_in_flight, ByteSize::ZERO, "the rows have left the live set");
	assert_eq!(carried, total, "so every byte is accounted to the batch in flight");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "and only settling releases them");
}

#[test]
fn the_full_trigger_fires_only_above_the_limit() {
	let limit = cost("alpha", Some("value"));
	let tier = tier(limit.as_bytes());
	let waker = Arc::new(CountingWaker::default());
	tier.attach_waker(waker.clone());

	tier.state().record(0, key("alpha"), Some(row("value")), 1);
	tier.observe_write();
	assert_eq!(waker.wakes.load(Ordering::SeqCst), 0, "a resident set exactly at the limit must not wake the host");
	assert!(!tier.is_triggered(), "and must not arm the trigger");

	tier.state().record(1, key("b"), Some(row("x")), 1);
	tier.observe_write();
	assert_eq!(waker.wakes.load(Ordering::SeqCst), 1, "the first byte past the limit must wake the host");
	assert!(tier.is_triggered(), "and must arm the trigger");
}

#[test]
fn a_burst_over_the_limit_raises_exactly_one_wake() {
	let tier = tier(cost("a", Some("x")).as_bytes());
	let waker = Arc::new(CountingWaker::default());
	tier.attach_waker(waker.clone());

	for name in ["a", "b", "c", "d", "e"] {
		tier.state().record(0, key(name), Some(row("x")), 1);
		tier.observe_write();
	}
	assert_eq!(
		waker.wakes.load(Ordering::SeqCst),
		1,
		"a burst over the limit must raise one wake, not one per write"
	);

	tier.flush_all();
	assert!(!tier.is_triggered(), "settling must consume the armed trigger");

	for name in ["f", "g"] {
		tier.state().record(0, key(name), Some(row("x")), 1);
		tier.observe_write();
	}
	assert_eq!(waker.wakes.load(Ordering::SeqCst), 2, "a fresh crossing after a settle must wake again");
}

#[test]
fn a_domain_that_refuses_over_budget_writes_stops_admitting_them() {
	let limit = cost("a", Some("x"));
	let open = tier(limit.as_bytes());
	let closed = floor_tier(limit.as_bytes());

	assert!(open.admits_write(), "an empty resident set admits writes under either policy");
	assert!(closed.admits_write(), "an empty resident set admits writes under either policy");

	open.state().record(0, key("a"), Some(row("xxxx")), 1);
	closed.state().record(0, key("a"), Some(row("xxxx")), 1);

	assert!(open.admits_write(), "the default policy keeps admitting once over budget");
	assert!(!closed.admits_write(), "a refusing domain must stop admitting once over budget");
}

#[test]
fn nothing_evictable_flushes_nothing() {
	let tier = roomy();
	tier.state().record(0, key("alpha"), Some(row("value")), 9);
	tier.state().set_watermark(None);

	let outcome = tier.flush_slice(ByteSize::from_mib(1));

	assert_eq!(outcome.slices, 0, "no cutoff means no slice");
	assert!(outcome.is_exhausted(), "and nothing left to paginate over");
	assert_eq!(tier.state().persists(), 0, "the persistent tier must not be opened at all");
	assert_eq!(tier.state().live_entries(0), 1, "the row stays resident");
}

#[test]
fn a_cutoff_leaves_later_writes_resident() {
	let tier = roomy();
	for version in 1..=4u64 {
		tier.state().record(0, key(&format!("k{version}")), Some(row("value")), version);
	}
	tier.state().set_watermark(Some(2));

	let outcome = tier.flush_pending();

	assert_eq!(outcome.persisted, 2, "only writes at or below the cutoff may be persisted");
	assert_eq!(tier.state().live_entries(0), 2, "the later writes stay resident");
	assert!(tier.state().persisted(0, &key("k2")).is_some(), "the write at the cutoff is included");
	assert!(tier.state().persisted(0, &key("k3")).is_none(), "the write past the cutoff is not");
	assert!(outcome.is_exhausted(), "nothing below the cutoff is left, so the pagination is done");
	assert_eq!(tier.state().live_bytes(), tier.state().census_bytes(), "the incremental counter must match a scan");
}

#[test]
fn a_drain_admits_everything_the_cutoff_held_back() {
	let tier = roomy();
	for version in 1..=4u64 {
		tier.state().record(0, key(&format!("k{version}")), Some(row("value")), version);
	}
	tier.state().set_watermark(Some(1));

	tier.flush_pending();
	assert_eq!(tier.state().live_entries(0), 3, "the running cutoff holds three writes back");

	let outcome = tier.flush_all();

	assert_eq!(outcome.persisted, 3, "the drain must take everything the cutoff held back");
	assert_eq!(tier.state().live_entries(0), 0, "and must leave nothing resident");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "and must release every byte it drained");
}

#[test]
fn a_slice_that_runs_out_of_budget_yields() {
	let one = cost("a", Some("value"));
	let tier = roomy();
	for kind in 0..2u8 {
		tier.state().record(kind, key("a"), Some(row("value")), 1);
	}

	let outcome = tier.flush_slice(one);

	assert!(outcome.is_yielded(), "a slice stopped by its byte budget must yield, not report exhaustion");
	assert_eq!(outcome.slices, 1, "and must have taken exactly what fit");
	assert_eq!(tier.metrics().budget_exhausted, 1, "and must record why it stopped");
}

#[test]
fn the_resume_cursor_serves_the_kind_a_slice_stopped_short_of() {
	let one = cost("a", Some("value"));
	let tier = tier(one.as_bytes());
	for kind in 0..3u8 {
		for name in ["a", "b", "c"] {
			tier.state().record(kind, key(name), Some(row("value")), 1);
		}
	}

	for _ in 0..3 {
		tier.flush_slice(one);
	}

	let mut served: Vec<(u8, u64)> =
		tier.kind_metrics().into_iter().map(|kind| (kind.kind, kind.counters.slices)).collect();
	served.sort();
	assert_eq!(
		served,
		vec![(0, 1), (1, 1), (2, 1)],
		"three slices must serve each kind once, not the first one thrice"
	);
}

#[test]
fn a_pagination_stops_at_the_slice_cap() {
	let one = cost("a", Some("value"));
	let tier = tier(one.as_bytes());
	for kind in 0..8u8 {
		for name in ["a", "b"] {
			tier.state().record(kind, key(name), Some(row("value")), 1);
		}
	}

	let outcome = tier.flush_pending();

	assert_eq!(outcome.slices, D::MAX_SLICES_PER_TICK as u64, "a pagination must stop at the cap");
	assert!(outcome.is_yielded(), "and must tell the host there is more");
	assert!(outcome.backlog > ByteSize::ZERO, "and must report what is still resident");
}

#[test]
fn one_slice_visits_every_kind_while_budget_remains() {
	let tier = roomy();
	for kind in 0..3u8 {
		for name in ["a", "b"] {
			tier.state().record(kind, key(name), Some(row("value")), 1);
		}
	}

	let outcome = tier.flush_pending();

	assert_eq!(outcome.slices, 3, "one slice with budget to spare must serve all three kinds");
	assert_eq!(outcome.persisted, 6, "and must take every entry they held");
	assert!(outcome.is_exhausted(), "an emptied resident set must report exhaustion");
	assert_eq!(outcome.backlog, ByteSize::ZERO, "and must report nothing left");
	assert_eq!(tier.state().persistent_entries(), 6, "and every entry must have reached the persistent tier");
}

#[test]
fn a_domain_below_its_transaction_floor_waits_for_the_tick() {
	let tier = floor_tier(ByteSize::from_mib(1).as_bytes());
	tier.state().record(0, key("a"), Some(row("small")), 1);
	assert!(tier.resident_bytes() < FLOOR, "the fixture must sit below the floor");

	let outcome = tier.flush_pending();

	assert_eq!(outcome.slices, 0, "a resident set below the floor is not worth a transaction");
	assert!(outcome.is_exhausted(), "and refusing must not ask the host to re-tick");
	assert_eq!(tier.state().persists(), 0, "and must not open one");
	assert_eq!(tier.state().selects(), 0, "and must not disturb the resident set");

	for index in 0..40u32 {
		tier.state().record(0, key(&format!("pad{index}")), Some(row("value")), 1);
	}
	assert!(tier.resident_bytes() >= FLOOR, "the fixture must now sit above the floor");

	let outcome = tier.flush_pending();
	assert!(outcome.persisted > 0, "once past the floor the same tier flushes");
}

#[test]
fn a_drain_ignores_the_transaction_floor() {
	let tier = floor_tier(ByteSize::from_mib(1).as_bytes());
	tier.state().record(0, key("a"), Some(row("small")), 1);

	let outcome = tier.flush_all();

	assert_eq!(outcome.persisted, 1, "the drain must take the sub-floor write");
	assert_eq!(tier.state().live_entries(0), 0, "and must leave nothing resident");
	assert_eq!(tier.resident_bytes(), ByteSize::ZERO, "and must release its bytes");
}

#[test]
fn a_key_the_persistent_tier_refused_returns_to_the_resident_set() {
	let tier = roomy();
	for name in ["a", "b", "c"] {
		tier.state().record(0, key(name), Some(row("value")), 1);
	}
	let total = tier.resident_bytes();
	let refused = cost("b", Some("value"));
	tier.state().refuse(key("b"));

	let outcome = tier.flush_pending();

	assert_eq!(outcome.persisted, 2, "only accepted keys count as persisted");
	assert_eq!(outcome.released, total.saturating_sub(refused), "and only their bytes are released");
	assert_eq!(tier.resident_bytes(), refused, "the refused key is still resident and still charged");
	assert_eq!(tier.state().live_entries(0), 1, "and is back in the live set");
	assert!(tier.state().persisted(0, &key("b")).is_none(), "and never reached the persistent tier");
	assert!(tier.state().persisted(0, &key("a")).is_some(), "while its accepted neighbours did");
	assert_eq!(tier.state().live_bytes(), tier.state().census_bytes(), "the incremental counter must match a scan");
}

#[test]
#[should_panic(expected = "commit tier persist failed")]
fn a_failed_persist_stops_the_process() {
	let tier = roomy();
	tier.state().record(0, key("a"), Some(row("value")), 1);
	tier.state().fail_persist(true);

	tier.flush_pending();
}

#[test]
fn metrics_report_what_a_flush_moved_and_what_is_left() {
	let tier = roomy();
	for name in ["a", "b", "c", "d"] {
		tier.state().record(0, key(name), Some(row("value")), 1);
	}
	tier.state().record(1, key("e"), Some(row("value")), 9);
	tier.state().set_watermark(Some(1));
	let flushed = cost("a", Some("value")) * 4;

	tier.flush_pending();

	let metrics = tier.metrics();
	assert_eq!(metrics.slices, 1, "one kind held everything below the cutoff");
	assert_eq!(metrics.persisted, 4, "and four entries went durable");
	assert_eq!(metrics.released, flushed, "and their bytes were released");
	assert_eq!(metrics.backlog, cost("e", Some("value")), "while the held-back write is still counted resident");
}

#[test]
fn metrics_are_labelled_per_kind() {
	let tier = roomy();
	tier.state().record(0, key("a"), Some(row("value")), 1);
	tier.state().record(1, key("b"), Some(row("value")), 1);

	tier.flush_pending();

	let mut labelled: Vec<(u8, u64)> =
		tier.kind_metrics().into_iter().map(|kind| (kind.kind, kind.counters.persisted)).collect();
	labelled.sort();
	assert_eq!(labelled, vec![(0, 1), (1, 1)], "each kind must carry its own counters");

	let mut samples = Vec::new();
	tier.collect(&mut samples);
	assert!(
		samples.iter().any(|sample| sample.scope == format!("{}::kind::k0", D::SCOPE)),
		"the domain's label must reach the metrics surface"
	);
	assert!(
		samples.iter().any(|sample| sample.scope == D::SCOPE && sample.metric == "resident_bytes"),
		"the scope must carry the tier's own residency"
	);
}

#[test]
fn a_mixed_workload_never_drifts_the_counter() {
	let tier = roomy();
	for index in 0..12u32 {
		tier.state().record(0, key(&format!("k{index:02}")), Some(row("value")), 1);
	}
	for index in 0..6u32 {
		tier.state().record(0, key(&format!("k{index:02}")), Some(row("longer-value")), 2);
	}
	tier.state().remove(0, key("k07"), 3);
	tier.state().drop_where(0, |candidate| !candidate.as_slice().ends_with(b"9"));
	tier.state().record(1, key("other"), Some(row("value")), 1);
	tier.state().refuse(key("k03"));
	tier.flush_pending();
	tier.state().record(0, key("k03"), Some(row("re-written")), 4);

	assert_eq!(tier.state().live_bytes(), tier.state().census_bytes(), "the incremental counter must match a scan");
	assert_eq!(
		tier.resident_bytes(),
		tier.state().live_bytes().saturating_add(tier.state().in_flight_bytes()),
		"the budget must always equal live plus in flight"
	);
	assert_eq!(tier.state().in_flight_bytes(), ByteSize::ZERO, "nothing is in flight once every batch has settled");
}
