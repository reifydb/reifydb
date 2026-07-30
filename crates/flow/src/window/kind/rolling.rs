// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::window::{
	policy::{EvictionPolicy, SealPolicy},
	span::WindowCoord,
};

pub struct RollingOverTime {
	size: Duration,
	lag: Duration,
}

impl RollingOverTime {
	pub fn new(size: Duration, lag: Duration) -> Self {
		Self {
			size,
			lag,
		}
	}

	pub fn span(&self) -> Duration {
		self.size.try_add(self.lag).unwrap_or(self.lag)
	}

	pub fn seal_policy(&self, grace: Duration) -> SealPolicy {
		SealPolicy::rolling(self.span(), grace)
	}

	pub fn eviction_policy(&self) -> EvictionPolicy {
		EvictionPolicy::rolling(self.span())
	}

	pub fn eviction_cutoff(&self, ledger: DateTime) -> DateTime {
		ledger.saturating_sub_span(self.span())
	}

	pub fn seal_horizon(&self, ledger: DateTime, grace: Duration) -> DateTime {
		ledger.saturating_sub_span(self.seal_policy(grace).admissible().duration())
	}
}

pub struct RollingOverRows {
	capacity: u64,
}

impl RollingOverRows {
	pub fn new(capacity: u64) -> Self {
		Self {
			capacity,
		}
	}

	pub fn capacity(&self) -> usize {
		self.capacity as usize
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	fn at(millis: u64) -> DateTime {
		DateTime::from_millis(millis)
	}

	#[test]
	fn a_row_capacity_has_no_lag_no_grace_and_no_horizon_to_ask_for() {
		// A rolling window over ROWS always has a current value, so it has no closed
		// state, no instant to seal at and no meaningful lag. Lag and grace are declared
		// in milliseconds, and subtracting milliseconds from a row number silently drops
		// rows in proportion to the lag - a 30s lag would demand 30000 rows of headroom.
		// RollingOverRows has no such method, so there is nothing to answer wrongly.
		let rows = RollingOverRows::new(64);

		assert_eq!(rows.capacity(), 64);
	}

	#[test]
	fn the_rolling_span_is_the_size_extended_by_the_lag() {
		// Lag shifts the whole window back in time, so a lagged rolling window must
		// RETAIN size + lag or the lagged read falls off the end of the buffer it is reading.
		// Both the eviction cutoff and the seal policy are built from this one span, which is
		// why it is computed once here rather than at each of them.
		assert_eq!(RollingOverTime::new(ms(5_000), ms(0)).span(), ms(5_000));
		assert_eq!(RollingOverTime::new(ms(5_000), ms(2_000)).span(), ms(7_000));
	}

	#[test]
	fn eviction_uses_the_bare_span_and_sealing_adds_the_grace() {
		// The asymmetry that made SealInstant and EvictionInstant separate types in the
		// first place. Rolling ADMITS a late row inside the grace but EVICTS on the bare span; an
		// eviction that also waited out the grace keeps every rolling window one grace-period too
		// wide, inflating every aggregate it publishes.
		let rolling = RollingOverTime::new(ms(5_000), ms(0));

		assert_eq!(rolling.eviction_cutoff(at(8_000)), at(3_000));
		assert_eq!(rolling.seal_horizon(at(8_000), ms(200)), at(2_800));
	}

	#[test]
	fn a_ledger_younger_than_the_span_clamps_to_the_epoch_instead_of_wrapping() {
		// At startup the ledger sits at or near the epoch while the span is minutes, so
		// this is the FIRST thing a fresh rolling window computes, not an edge case. An
		// underflow here produces a cutoff near the maximum instant, which evicts every row the
		// window has ever held on its first tick.
		let rolling = RollingOverTime::new(ms(5_000), ms(0));

		assert_eq!(rolling.eviction_cutoff(at(0)), at(0));
		assert_eq!(rolling.eviction_cutoff(at(1_000)), at(0));
		assert_eq!(rolling.seal_horizon(at(1_000), ms(200)), at(0));
	}

	#[test]
	fn the_seal_horizon_never_sits_later_than_the_eviction_cutoff() {
		// Sealing decides what may still be AMENDED, eviction decides what is still
		// STORED. A horizon later than the cutoff means a window is declared sealed while its
		// rows are still retained, so a late row is refused for a window that could still have
		// accepted it. The grace is what keeps the horizon behind, and this holds it across the
		// whole lag/grace space rather than at one point.
		for lag in [ms(0), ms(1), ms(9_000)] {
			for grace in [ms(0), ms(1), ms(9_000)] {
				let rolling = RollingOverTime::new(ms(5_000), lag);
				assert!(
					rolling.seal_horizon(at(60_000), grace) <= rolling.eviction_cutoff(at(60_000)),
					"horizon passed the cutoff at lag {lag:?} grace {grace:?}"
				);
			}
		}
	}
}
