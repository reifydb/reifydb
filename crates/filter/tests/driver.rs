// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_filter::{
	adaptive::AdaptiveKeyFilter,
	config::FilterConfig,
	driver::{DriverProgress, RebuildDriver},
	source::{FilterSlice, KeyFilterSource},
};
use reifydb_runtime::sync::mutex::Mutex;

#[derive(Default, Debug)]
struct SourceState {
	hashes: Vec<u64>,
	pos: usize,
	restarts: usize,
	slices: usize,
}

#[derive(Clone, Debug)]
struct FakeSource {
	state: Arc<Mutex<SourceState>>,
}

impl FakeSource {
	fn new(hashes: Vec<u64>) -> Self {
		// The driver owns one clone and the test keeps another, so the key set can be mutated
		// between steps to simulate deletions and concurrent writes against a live scan.
		Self {
			state: Arc::new(Mutex::new(SourceState {
				hashes,
				pos: 0,
				restarts: 0,
				slices: 0,
			})),
		}
	}

	fn set_keys(&self, hashes: Vec<u64>) {
		self.state.lock().hashes = hashes;
	}

	fn restarts(&self) -> usize {
		self.state.lock().restarts
	}

	fn slices(&self) -> usize {
		self.state.lock().slices
	}
}

impl KeyFilterSource for FakeSource {
	fn name(&self) -> &'static str {
		"fake"
	}

	fn estimated_len(&self) -> u64 {
		self.state.lock().hashes.len() as u64
	}

	fn restart(&mut self) {
		let mut state = self.state.lock();
		state.pos = 0;
		state.restarts += 1;
	}

	fn next_slice(&mut self, budget: usize) -> FilterSlice {
		let mut state = self.state.lock();
		state.slices += 1;
		let end = (state.pos + budget).min(state.hashes.len());
		let hashes = state.hashes[state.pos..end].to_vec();
		state.pos = end;
		let exhausted = state.pos >= state.hashes.len();
		FilterSlice {
			hashes,
			exhausted,
		}
	}
}

fn key(i: u64) -> u64 {
	i.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x5DEE_CE66_D
}

fn keys(range: std::ops::Range<u64>) -> Vec<u64> {
	range.map(key).collect()
}

fn never_triggering() -> FilterConfig {
	// Both triggers pushed out of reach so a test that observes a rebuild proves it came from the
	// disabled-filter branch and not from fill or drift.
	FilterConfig {
		fill_trigger: 1.5,
		drift_trigger: 1.0e30,
		..FilterConfig::default()
	}
}

fn run_to_commit(driver: &mut RebuildDriver) -> Vec<DriverProgress> {
	// Drives one full cycle and records every step so a caller can assert the exact shape of the
	// sequence; the cap turns a driver that never commits into a failure instead of a hang.
	let mut seen = Vec::new();
	for _ in 0..10_000 {
		let progress = driver.step();
		seen.push(progress);
		if progress == DriverProgress::Committed {
			return seen;
		}
	}
	panic!("driver never committed: {:?}", seen);
}

#[test]
fn disabled_filter_starts_a_rebuild_on_the_first_step() {
	// A filter with no active bloom answers "may contain everything", so it saves nothing until the
	// first swap. That initial build must not wait for a fill or drift trigger, neither of which can
	// ever fire on a filter that has never been built.
	let filter = Arc::new(AdaptiveKeyFilter::new());
	let source = FakeSource::new(keys(0..10));

	let mut driver = RebuildDriver::new(filter.clone(), Box::new(source), never_triggering());

	assert!(!filter.is_enabled());
	assert_eq!(driver.step(), DriverProgress::Started);
}

#[test]
fn a_full_cycle_is_started_then_scanning_then_exactly_one_committed() {
	// The step sequence is the contract the actor schedules against: Started once, Scanning while
	// work remains, and a single Committed that ends the cycle. A second Committed would mean the
	// handle survived the swap; a missing one would leave the rebuild in flight forever.
	let filter = Arc::new(AdaptiveKeyFilter::new());
	let live = keys(0..10);
	let source = FakeSource::new(live.clone());
	let config = FilterConfig {
		scan_budget: 3,
		..never_triggering()
	};

	let mut driver = RebuildDriver::new(filter.clone(), Box::new(source), config);
	let sequence = run_to_commit(&mut driver);

	assert_eq!(
		sequence,
		vec![
			DriverProgress::Started,
			DriverProgress::Scanning,
			DriverProgress::Scanning,
			DriverProgress::Scanning,
			DriverProgress::Committed,
		]
	);
	assert_eq!(sequence.iter().filter(|p| **p == DriverProgress::Committed).count(), 1);

	assert!(filter.is_enabled(), "a committed rebuild must leave the filter enabled");
	for hash in &live {
		assert!(filter.may_contain(*hash), "key {} the source held answered absent after the swap", hash);
	}
	assert!(!filter.metrics().rebuilding);
	assert_eq!(filter.metrics().rebuilds, 1);
}

#[test]
fn a_committed_cycle_is_followed_by_idle_when_no_trigger_fires() {
	// Without this the driver would restart a rebuild on the very next tick and scan the whole key
	// space continuously, which is the cost the interval exists to bound.
	let filter = Arc::new(AdaptiveKeyFilter::new());
	let source = FakeSource::new(keys(0..100));

	let mut driver = RebuildDriver::new(filter.clone(), Box::new(source.clone()), FilterConfig::default());
	run_to_commit(&mut driver);

	let slices_after_build = source.slices();
	let restarts_after_build = source.restarts();

	assert_eq!(driver.step(), DriverProgress::Idle);
	assert_eq!(driver.step(), DriverProgress::Idle);

	assert_eq!(source.slices(), slices_after_build, "an idle step fetched a slice");
	assert_eq!(source.restarts(), restarts_after_build, "an idle step restarted the source");
	assert_eq!(filter.metrics().rebuilds, 1);
}

#[test]
fn dual_write_is_live_before_the_first_slice_is_ever_fetched() {
	// The ordering invariant. Dual-write begins at begin_rebuild, so begin_rebuild must happen before
	// the scan reads anything: a key written after the cursor passed its position but before the
	// rebuild began would land in neither the scan output nor the new filter, and the swap would then
	// answer "definitely absent" for a row that exists. Asserting slices() == 0 alongside rebuilding
	// is what makes this fail if begin_rebuild is moved after the first next_slice.
	let filter = Arc::new(AdaptiveKeyFilter::new());
	let source = FakeSource::new(keys(0..1000));
	let config = FilterConfig {
		scan_budget: 1,
		..never_triggering()
	};

	let mut driver = RebuildDriver::new(filter.clone(), Box::new(source.clone()), config);

	assert_eq!(driver.step(), DriverProgress::Started);
	assert!(filter.metrics().rebuilding, "dual-write was not live when Started was returned");
	assert_eq!(source.slices(), 0, "a slice was fetched in the step that started the rebuild");
	assert_eq!(source.restarts(), 1, "the source was not rewound before the scan");
}

#[test]
fn a_rebuild_reclaims_the_space_held_by_deleted_keys() {
	// The reason the whole driver exists: a bloom cannot delete, so removed keys stay as permanent
	// false positives. After a rebuild over the shrunken source the removed keys must overwhelmingly
	// answer absent. A handful can still collide into the live keys' bits, so the assertion is on the
	// fraction surviving rather than on any single key.
	let filter = Arc::new(AdaptiveKeyFilter::new());
	let all = keys(0..4000);
	let source = FakeSource::new(all.clone());
	let config = FilterConfig {
		scan_budget: 500,
		size_headroom: 1.0,
		min_size_keys: 1,
		..FilterConfig::default()
	};

	let mut driver = RebuildDriver::new(filter.clone(), Box::new(source.clone()), config);
	run_to_commit(&mut driver);

	let live: Vec<u64> = all[..400].to_vec();
	let removed: Vec<u64> = all[400..].to_vec();
	for hash in &removed {
		assert!(filter.may_contain(*hash), "setup failed: {} was not in the filter to begin with", hash);
	}

	source.set_keys(live.clone());
	assert!(filter.metrics().fill_ratio > FilterConfig::default().fill_trigger, "setup failed to arm the trigger");
	assert_eq!(driver.step(), DriverProgress::Started);
	run_to_commit(&mut driver);

	for hash in &live {
		assert!(filter.may_contain(*hash), "live key {} was lost by the reclaiming rebuild", hash);
	}
	let still_present = removed.iter().filter(|hash| filter.may_contain(**hash)).count();
	let fraction = still_present as f64 / removed.len() as f64;
	assert!(fraction < 0.05, "{} of {} deleted keys still report present", still_present, removed.len());
}

#[test]
fn a_key_written_mid_rebuild_survives_the_swap() {
	// Dual-write end to end through the driver: a writer that adds a key after Started but before
	// Committed is exactly the race the ordering invariant protects. The control filter runs the same
	// cycle without the mid-rebuild write, proving the key is not simply a bloom false positive.
	let written = key(999_999);

	let control_filter = Arc::new(AdaptiveKeyFilter::new());
	let mut control = RebuildDriver::new(
		control_filter.clone(),
		Box::new(FakeSource::new(keys(0..4000))),
		FilterConfig {
			scan_budget: 500,
			size_headroom: 1.0,
			min_size_keys: 1,
			..FilterConfig::default()
		},
	);
	run_to_commit(&mut control);
	assert!(!control_filter.may_contain(written), "control key collides; pick another and retest");

	let filter = Arc::new(AdaptiveKeyFilter::new());
	let mut driver = RebuildDriver::new(
		filter.clone(),
		Box::new(FakeSource::new(keys(0..4000))),
		FilterConfig {
			scan_budget: 500,
			size_headroom: 1.0,
			min_size_keys: 1,
			..FilterConfig::default()
		},
	);

	assert_eq!(driver.step(), DriverProgress::Started);
	filter.add(written);
	assert_eq!(driver.step(), DriverProgress::Scanning);
	filter.add(written);
	run_to_commit(&mut driver);

	assert!(filter.may_contain(written), "a key written during the rebuild was lost by the swap");
}

#[test]
fn drift_trigger_fires_when_the_filter_holds_far_more_keys_than_the_source() {
	// The production symptom: a filter estimating 1.5M keys over a table holding 357k rows. Fill alone
	// does not catch that, so drift must, or a filter that is mostly garbage is never rebuilt.
	let filter = Arc::new(AdaptiveKeyFilter::new());
	let source = FakeSource::new(keys(0..4000));
	let config = FilterConfig {
		scan_budget: 1000,
		size_headroom: 1.0,
		min_size_keys: 1,
		fill_trigger: 0.9,
		drift_trigger: 2.0,
		..FilterConfig::default()
	};

	let mut driver = RebuildDriver::new(filter.clone(), Box::new(source.clone()), config);
	run_to_commit(&mut driver);

	source.set_keys(keys(0..100));

	let metrics = filter.metrics();
	assert!(metrics.fill_ratio <= config.fill_trigger, "fill would have fired; this no longer tests drift");
	assert!((metrics.estimated_keys as f64) > 100.0 * config.drift_trigger);
	assert_eq!(driver.step(), DriverProgress::Started);
}

#[test]
fn fill_trigger_fires_on_a_saturated_filter_when_drift_is_not_extreme() {
	// An undersized filter saturates and its false-positive rate goes to one, which is useless even
	// though the key set never drifted. Fill has to catch that on its own.
	let filter = Arc::new(AdaptiveKeyFilter::new());
	let source = FakeSource::new(keys(0..4000));
	let config = FilterConfig {
		scan_budget: 1000,
		size_headroom: 0.05,
		min_size_keys: 1,
		fill_trigger: 0.4,
		drift_trigger: 1.0e30,
		..FilterConfig::default()
	};

	let mut driver = RebuildDriver::new(filter.clone(), Box::new(source.clone()), config);
	run_to_commit(&mut driver);

	let metrics = filter.metrics();
	assert!(metrics.fill_ratio > config.fill_trigger, "setup failed to saturate: {}", metrics.fill_ratio);
	assert!(
		(metrics.estimated_keys as f64) <= 4000.0 * config.drift_trigger,
		"drift would have fired; this no longer tests fill"
	);
	assert_eq!(driver.step(), DriverProgress::Started);
}

#[test]
fn a_healthy_filter_stays_idle_without_touching_the_source() {
	// A rebuild is a full scan of the key space. Starting one when neither trigger fires would turn a
	// bounded maintenance cost into a continuous one, so an idle decision must cost no scan at all.
	let filter = Arc::new(AdaptiveKeyFilter::new());
	let source = FakeSource::new(keys(0..100));

	let mut driver = RebuildDriver::new(filter.clone(), Box::new(source.clone()), FilterConfig::default());
	run_to_commit(&mut driver);

	let metrics = filter.metrics();
	assert!(metrics.fill_ratio <= FilterConfig::default().fill_trigger);
	assert!((metrics.estimated_keys as f64) <= 100.0 * FilterConfig::default().drift_trigger);

	let slices = source.slices();
	let restarts = source.restarts();
	for _ in 0..5 {
		assert_eq!(driver.step(), DriverProgress::Idle);
	}
	assert_eq!(source.slices(), slices, "an idle decision fetched a slice");
	assert_eq!(source.restarts(), restarts, "an idle decision restarted the source");
}

#[test]
fn the_new_filter_is_sized_from_the_source_times_headroom() {
	// Sizing off anything but the live key count reintroduces the problem the rebuild solves: too small
	// and it saturates immediately, too large and it wastes memory forever.
	let filter = Arc::new(AdaptiveKeyFilter::new());
	let source = FakeSource::new(keys(0..500));
	let config = FilterConfig {
		scan_budget: 1000,
		size_headroom: 2.0,
		min_size_keys: 8,
		..never_triggering()
	};

	let mut driver = RebuildDriver::new(filter.clone(), Box::new(source), config);
	run_to_commit(&mut driver);

	// 500 keys * 2.0 headroom = 1000 keys, and a bloom allocates ten bits per key rounded up to a word.
	let size_bits = filter.metrics().size_bits;
	assert!(size_bits >= 10_000, "size_bits {} ignored the headroom", size_bits);
	assert!(size_bits < 10_064, "size_bits {} is larger than the requested 1000 keys", size_bits);
}

#[test]
fn an_empty_source_still_produces_a_usable_filter() {
	// Zero keys times any headroom is zero, and a zero-sized filter is either a panic or a filter that
	// answers for nothing. The floor is what keeps a table that is momentarily empty from producing one.
	let filter = Arc::new(AdaptiveKeyFilter::new());
	let source = FakeSource::new(Vec::new());
	let config = FilterConfig {
		min_size_keys: 1024,
		size_headroom: 2.0,
		..never_triggering()
	};

	let mut driver = RebuildDriver::new(filter.clone(), Box::new(source), config);
	assert_eq!(run_to_commit(&mut driver), vec![DriverProgress::Started, DriverProgress::Committed]);

	assert!(filter.is_enabled());
	let size_bits = filter.metrics().size_bits;
	assert_eq!(size_bits, 10_240, "an empty source did not fall back to the min_size_keys floor");
	assert!(!filter.may_contain(key(7)), "a filter built from an empty source must rule everything out");
}

#[test]
fn sizing_ignores_a_saturated_estimated_key_count() {
	// estimated_items saturates to usize::MAX once fill_ratio reaches 1.0, so metrics.estimated_keys
	// becomes u64::MAX. That is the right answer for the drift trigger but nonsense as a size: sizing
	// the next filter from it would ask for roughly 1.8e19 keys.
	let filter = Arc::new(AdaptiveKeyFilter::new());
	let source = FakeSource::new(keys(0..4));
	let config = FilterConfig {
		scan_budget: 1000,
		size_headroom: 2.0,
		min_size_keys: 8,
		fill_trigger: 0.4,
		drift_trigger: 1.0e30,
		..FilterConfig::default()
	};

	let mut driver = RebuildDriver::new(filter.clone(), Box::new(source.clone()), config);
	assert_eq!(driver.step(), DriverProgress::Started);
	source.set_keys(keys(0..4000));
	run_to_commit(&mut driver);

	assert_eq!(filter.metrics().estimated_keys, u64::MAX, "setup failed to saturate the active filter");

	assert_eq!(driver.step(), DriverProgress::Started);
	run_to_commit(&mut driver);

	// 4000 live keys * 2.0 headroom = 8000 keys at ten bits each, not 1.8e19 keys.
	let size_bits = filter.metrics().size_bits;
	assert!(size_bits >= 80_000, "size_bits {} ignored the source length", size_bits);
	assert!(size_bits < 80_064, "size_bits {} was sized from the saturated estimate", size_bits);
}
