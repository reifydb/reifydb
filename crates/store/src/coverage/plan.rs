// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::typed::{ExclusiveUpperEnd, Key, MultiKey};

use crate::coverage::interval::{CoverageSet, Interval};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Segment<K = MultiKey> {
	Resident(Interval<K>),
	Gap {
		interval: Interval<K>,
		exempt: bool,
	},
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanPlan<K = MultiKey> {
	pub segments: Vec<Segment<K>>,
	pub gaps: usize,
	pub exempted: usize,
	pub degraded: bool,
}

impl<K: Key> ScanPlan<K> {
	pub fn full(interval: Interval<K>) -> Self {
		Self {
			segments: vec![Segment::Gap {
				interval,
				exempt: false,
			}],
			gaps: 1,
			exempted: 0,
			degraded: true,
		}
	}

	fn empty() -> Self {
		Self {
			segments: Vec::new(),
			gaps: 0,
			exempted: 0,
			degraded: false,
		}
	}
}

pub const DEFAULT_GAP_GUARD: usize = 4;

pub fn plan<K, F>(coverage: &CoverageSet<K>, lo: K, hi: ExclusiveUpperEnd<K>, guard: usize, exempt: F) -> ScanPlan<K>
where
	K: Key,
	F: Fn(&Interval<K>) -> bool,
{
	if !hi.covers(&lo) {
		return ScanPlan::empty();
	}

	let resident = coverage.overlapping(&lo, &hi);
	let holes = coverage.gaps(&lo, &hi);

	let mut segments = Vec::with_capacity(resident.len() + holes.len());
	let mut gaps = 0;
	let mut exempt_gaps = 0;

	let mut resident = resident.into_iter().peekable();
	let mut holes = holes.into_iter().peekable();

	loop {
		let resident_first = match (resident.peek(), holes.peek()) {
			(Some(left), Some(right)) => left.start <= right.start,
			(Some(_), None) => true,
			(None, Some(_)) => false,
			(None, None) => break,
		};

		if resident_first {
			match resident.next() {
				Some(interval) => segments.push(Segment::Resident(interval)),
				None => break,
			}
		} else {
			match holes.next() {
				Some(interval) => {
					let exempt = exempt(&interval);
					gaps += 1;
					if exempt {
						exempt_gaps += 1;
					}
					segments.push(Segment::Gap {
						interval,
						exempt,
					});
				}
				None => break,
			}
		}
	}

	if gaps - exempt_gaps > guard {
		return ScanPlan::full(Interval::new(lo, hi));
	}

	ScanPlan {
		segments,
		gaps,
		exempted: exempt_gaps,
		degraded: false,
	}
}

const HISTOGRAM_SLOTS: usize = 8;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GapHistogram {
	slots: [u64; HISTOGRAM_SLOTS],
	scans: u64,
	degraded: u64,
}

impl GapHistogram {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn record<K: Key>(&mut self, plan: &ScanPlan<K>) {
		let count = plan.gaps - plan.exempted;
		let bounds = Self::bounds();
		let slot = bounds.iter().rposition(|bound| count >= *bound).unwrap_or(0);

		self.slots[slot] += 1;
		self.scans += 1;
		if plan.degraded {
			self.degraded += 1;
		}
	}

	pub fn merge(&mut self, other: &GapHistogram) {
		for (slot, count) in self.slots.iter_mut().zip(other.slots.iter()) {
			*slot += *count;
		}
		self.scans += other.scans;
		self.degraded += other.degraded;
	}

	pub fn bounds() -> [usize; HISTOGRAM_SLOTS] {
		[0, 1, 2, 3, 4, 5, 9, 17]
	}

	pub fn slots(&self) -> [u64; HISTOGRAM_SLOTS] {
		self.slots
	}

	pub fn scans(&self) -> u64 {
		self.scans
	}

	pub fn degraded(&self) -> u64 {
		self.degraded
	}

	pub fn median(&self) -> Option<usize> {
		if self.scans == 0 {
			return None;
		}

		let half = self.scans.div_ceil(2);
		let bounds = Self::bounds();
		let mut seen = 0;

		for (slot, count) in self.slots.iter().enumerate() {
			seen += *count;
			if seen >= half {
				return Some(bounds[slot]);
			}
		}

		None
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::key::typed::ExclusiveUpperEnd;

	use super::{DEFAULT_GAP_GUARD, GapHistogram, ScanPlan, Segment, plan};
	use crate::coverage::interval::{CoverageSet, Interval};

	fn key(bytes: &[u8]) -> EncodedKey {
		EncodedKey::new(bytes)
	}

	fn interval_of(segment: &Segment) -> &Interval {
		match segment {
			Segment::Resident(interval) => interval,
			Segment::Gap {
				interval,
				..
			} => interval,
		}
	}

	fn is_gap(segment: &Segment) -> bool {
		matches!(segment, Segment::Gap { .. })
	}

	fn counted(gaps: usize, exempt_gaps: usize, degraded: bool) -> ScanPlan {
		ScanPlan {
			segments: Vec::new(),
			gaps,
			exempted: exempt_gaps,
			degraded,
		}
	}

	fn punched() -> CoverageSet {
		let mut coverage = CoverageSet::new();
		coverage.extend(key(b"b"), ExclusiveUpperEnd::of(b"c"));
		coverage.extend(key(b"d"), ExclusiveUpperEnd::of(b"e"));
		coverage.extend(key(b"f"), ExclusiveUpperEnd::of(b"g"));
		coverage.extend(key(b"h"), ExclusiveUpperEnd::of(b"i"));
		coverage.extend(key(b"j"), ExclusiveUpperEnd::of(b"k"));
		coverage
	}

	#[test]
	fn full_plan_is_one_non_exempt_gap_spanning_the_whole_range() {
		// The guard fallback must be a single scan the caller can install as one interval.
		let plan = ScanPlan::full(Interval::new(key(b"a"), ExclusiveUpperEnd::of(b"m")));

		assert_eq!(plan.segments.len(), 1);
		assert_eq!(
			plan.segments[0],
			Segment::Gap {
				interval: Interval::new(key(b"a"), ExclusiveUpperEnd::of(b"m")),
				exempt: false,
			}
		);
		assert_eq!(plan.gaps, 1);
		assert_eq!(plan.exempted, 0);
		assert!(plan.degraded);
	}

	#[test]
	fn empty_coverage_plans_one_gap_over_the_whole_range() {
		// Nothing resident must still answer the range, from the persistent tier alone.
		let coverage = CoverageSet::new();

		let plan = plan(&coverage, key(b"a"), ExclusiveUpperEnd::of(b"m"), DEFAULT_GAP_GUARD, |_| false);

		assert_eq!(plan.segments.len(), 1);
		assert_eq!(interval_of(&plan.segments[0]), &Interval::new(key(b"a"), ExclusiveUpperEnd::of(b"m")));
		assert!(is_gap(&plan.segments[0]));
		assert_eq!(plan.gaps, 1);
		assert_eq!(plan.exempted, 0);
		assert!(!plan.degraded);
	}

	#[test]
	fn total_coverage_plans_one_ram_segment_and_no_gaps() {
		// A fully covered range must never touch the persistent tier.
		let mut coverage = CoverageSet::new();
		coverage.extend(key(b"a"), ExclusiveUpperEnd::Top);

		let plan = plan(&coverage, key(b"a"), ExclusiveUpperEnd::of(b"m"), DEFAULT_GAP_GUARD, |_| false);

		assert_eq!(
			plan.segments,
			vec![Segment::Resident(Interval::new(key(b"a"), ExclusiveUpperEnd::of(b"m")))]
		);
		assert_eq!(plan.gaps, 0);
		assert_eq!(plan.exempted, 0);
		assert!(!plan.degraded);
	}

	#[test]
	fn plan_alternates_ram_and_gap_leaving_no_hole() {
		// Segments must tile [lo, hi) exactly once: any hole or overlap loses or duplicates rows.
		let mut coverage = CoverageSet::new();
		coverage.extend(key(b"a"), ExclusiveUpperEnd::of(b"d"));
		coverage.extend(key(b"f"), ExclusiveUpperEnd::of(b"h"));

		let plan = plan(&coverage, key(b"a"), ExclusiveUpperEnd::of(b"m"), DEFAULT_GAP_GUARD, |_| false);

		assert_eq!(
			plan.segments,
			vec![
				Segment::Resident(Interval::new(key(b"a"), ExclusiveUpperEnd::of(b"d"))),
				Segment::Gap {
					interval: Interval::new(key(b"d"), ExclusiveUpperEnd::of(b"f")),
					exempt: false,
				},
				Segment::Resident(Interval::new(key(b"f"), ExclusiveUpperEnd::of(b"h"))),
				Segment::Gap {
					interval: Interval::new(key(b"h"), ExclusiveUpperEnd::of(b"m")),
					exempt: false,
				},
			]
		);

		assert_eq!(interval_of(&plan.segments[0]).start, key(b"a"));
		assert_eq!(interval_of(plan.segments.last().unwrap()).end, ExclusiveUpperEnd::of(b"m"));
		for pair in plan.segments.windows(2) {
			assert_eq!(
				interval_of(&pair[0]).end,
				ExclusiveUpperEnd::Key(interval_of(&pair[1]).start.clone())
			);
			assert_ne!(is_gap(&pair[0]), is_gap(&pair[1]));
		}
	}

	#[test]
	fn exempt_gaps_do_not_trip_the_guard() {
		// Counting permanently uncacheable spans would degrade every group-wide scan forever.
		let coverage = punched();

		let plan = plan(&coverage, key(b"a"), ExclusiveUpperEnd::of(b"m"), 1, |interval| {
			interval.start != key(b"a")
		});

		assert_eq!(plan.gaps, 6);
		assert_eq!(plan.exempted, 5);
		assert!(!plan.degraded);
		assert_eq!(plan.segments.len(), 11);
	}

	#[test]
	fn non_exempt_gaps_beyond_the_guard_degrade_to_one_full_scan() {
		// Twenty small persistent round trips are worse than no cache, so the plan is abandoned.
		let coverage = punched();

		let plan = plan(&coverage, key(b"a"), ExclusiveUpperEnd::of(b"m"), DEFAULT_GAP_GUARD, |_| false);

		assert!(plan.degraded);
		assert_eq!(
			plan.segments,
			vec![Segment::Gap {
				interval: Interval::new(key(b"a"), ExclusiveUpperEnd::of(b"m")),
				exempt: false,
			}]
		);
		assert_eq!(plan.gaps, 1);
		assert_eq!(plan.exempted, 0);
	}

	#[test]
	fn non_exempt_gaps_exactly_at_the_guard_are_served() {
		// The budget is a maximum, not a threshold: exceeding it degrades, meeting it must not.
		let coverage = punched();

		let plan = plan(&coverage, key(b"a"), ExclusiveUpperEnd::of(b"m"), 6, |_| false);

		assert!(!plan.degraded);
		assert_eq!(plan.gaps, 6);
		assert_eq!(plan.segments.len(), 11);
	}

	#[test]
	fn unbounded_upper_end_keeps_its_trailing_gap_open() {
		// A scan to Edge::Top must end in a gap that stays unbounded, not one clipped to a key.
		let mut coverage = CoverageSet::new();
		coverage.extend(key(b"d"), ExclusiveUpperEnd::of(b"f"));

		let plan = plan(&coverage, key(b"a"), ExclusiveUpperEnd::Top, DEFAULT_GAP_GUARD, |_| false);

		assert_eq!(
			plan.segments,
			vec![
				Segment::Gap {
					interval: Interval::new(key(b"a"), ExclusiveUpperEnd::of(b"d")),
					exempt: false,
				},
				Segment::Resident(Interval::new(key(b"d"), ExclusiveUpperEnd::of(b"f"))),
				Segment::Gap {
					interval: Interval::new(key(b"f"), ExclusiveUpperEnd::Top),
					exempt: false,
				},
			]
		);
		assert_eq!(plan.gaps, 2);
	}

	#[test]
	fn empty_range_plans_nothing() {
		// An inverted or degenerate range must emit no segment, never a gap the caller would scan.
		let coverage = punched();

		let inverted = plan(&coverage, key(b"m"), ExclusiveUpperEnd::of(b"a"), DEFAULT_GAP_GUARD, |_| false);
		assert!(inverted.segments.is_empty());
		assert_eq!(inverted.gaps, 0);
		assert!(!inverted.degraded);

		let degenerate = plan(&coverage, key(b"d"), ExclusiveUpperEnd::of(b"d"), DEFAULT_GAP_GUARD, |_| false);
		assert!(degenerate.segments.is_empty());
		assert_eq!(degenerate.gaps, 0);
		assert!(!degenerate.degraded);
	}

	#[test]
	fn histogram_buckets_by_non_exempt_gap_count() {
		// Exempt gaps are not work the guard cares about, so they must not shift the bucket.
		let mut histogram = GapHistogram::new();
		histogram.record(&counted(5, 3, false));
		histogram.record(&counted(3, 0, false));

		assert_eq!(histogram.slots(), [0, 0, 1, 1, 0, 0, 0, 0]);
		assert_eq!(histogram.scans(), 2);
	}

	#[test]
	fn histogram_wide_slots_absorb_their_whole_range() {
		// Slots 5 and 6 span 5..=8 and 9..=16; a count landing outside them mislabels the tail.
		let mut histogram = GapHistogram::new();
		for gaps in [5, 6, 7, 8] {
			histogram.record(&counted(gaps, 0, false));
		}
		histogram.record(&counted(9, 0, false));
		histogram.record(&counted(16, 0, false));
		histogram.record(&counted(17, 0, false));
		histogram.record(&counted(1000, 0, false));

		assert_eq!(histogram.slots(), [0, 0, 0, 0, 0, 4, 2, 2]);
		assert_eq!(histogram.scans(), 8);
	}

	#[test]
	fn histogram_counts_degraded_plans_separately() {
		// Degraded scans still belong in a bucket, but the degrade rate is the guard's signal.
		let mut histogram = GapHistogram::new();
		histogram.record(&counted(1, 0, true));
		histogram.record(&counted(1, 0, true));
		histogram.record(&counted(1, 0, false));

		assert_eq!(histogram.degraded(), 2);
		assert_eq!(histogram.scans(), 3);
		assert_eq!(histogram.slots()[1], 3);
	}

	#[test]
	fn histogram_median_is_none_without_scans() {
		// An empty histogram has no median to report; zero would read as a measured result.
		assert_eq!(GapHistogram::new().median(), None);
	}

	#[test]
	fn histogram_median_is_where_half_the_scans_have_accumulated() {
		// The median must be the first slot whose cumulative count reaches half, not the last slot hit.
		let mut histogram = GapHistogram::new();
		histogram.record(&counted(1, 0, false));
		histogram.record(&counted(2, 0, false));
		histogram.record(&counted(3, 0, false));
		histogram.record(&counted(10, 0, false));

		assert_eq!(histogram.median(), Some(2));
	}

	#[test]
	fn histogram_median_reports_the_lower_bound_of_a_wide_slot() {
		// Inside 9..=16 only the bound is known; reporting a recorded count would invent precision.
		let mut histogram = GapHistogram::new();
		histogram.record(&counted(12, 0, false));
		histogram.record(&counted(13, 0, false));

		assert_eq!(histogram.median(), Some(9));
	}

	#[test]
	fn histogram_merge_sums_slots_scans_and_degraded() {
		// Per-shard histograms are only readable if merging keeps every total intact.
		let mut left = GapHistogram::new();
		left.record(&counted(0, 0, false));
		left.record(&counted(2, 0, true));

		let mut right = GapHistogram::new();
		right.record(&counted(2, 0, false));
		right.record(&counted(9, 0, true));

		left.merge(&right);

		assert_eq!(left.slots(), [1, 0, 2, 0, 0, 0, 1, 0]);
		assert_eq!(left.scans(), 4);
		assert_eq!(left.degraded(), 2);
	}
}
