// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::{
	operator::state::seal::{
		coord::Coord,
		policy::{EvictionPolicy, SealPolicy},
	},
	window::coord::RowSpan,
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

	pub fn seal_policy(&self, lateness: Duration) -> SealPolicy {
		SealPolicy::rolling(self.span(), lateness)
	}

	pub fn eviction_policy(&self) -> EvictionPolicy {
		EvictionPolicy::rolling(self.span())
	}

	pub fn eviction_cutoff(&self, ledger: DateTime) -> Option<DateTime> {
		ledger.checked_sub_span(self.span())
	}

	pub fn seal_horizon(&self, ledger: DateTime, lateness: Duration) -> DateTime {
		ledger.saturating_sub_span(self.seal_policy(lateness).admissible().duration())
	}
}

pub struct RollingOverRows {
	capacity: RowSpan,
}

impl RollingOverRows {
	pub fn new(capacity: RowSpan) -> Self {
		Self {
			capacity,
		}
	}

	pub fn capacity(&self) -> usize {
		self.capacity.rows() as usize
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::factory::time::at_millis;

	use super::*;

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
	}

	#[test]
	fn a_row_capacity_has_no_lag_no_lateness_and_no_horizon_to_ask_for() {
		// A rolling window over ROWS always has a current value, so there is no lag, lateness or seal
		// instant to ask for. Lag and lateness are milliseconds, and subtracting them from a row number
		// would drop rows in proportion to the lag; the type carries no such method to answer wrongly.
		let rows = RollingOverRows::new(RowSpan::of(64));

		assert_eq!(rows.capacity(), 64);
	}

	#[test]
	fn the_rolling_span_is_the_size_extended_by_the_lag() {
		// Lag shifts the whole window back in time, so a lagged rolling window must retain size + lag
		// or the lagged read falls off the end of the buffer. Eviction cutoff and seal policy are
		// both built from this one span.
		assert_eq!(RollingOverTime::new(ms(5_000), ms(0)).span(), ms(5_000));
		assert_eq!(RollingOverTime::new(ms(5_000), ms(2_000)).span(), ms(7_000));
	}

	#[test]
	fn eviction_uses_the_bare_span_and_sealing_adds_the_lateness() {
		// Rolling admits a late row inside the lateness but evicts on the bare span. An eviction that
		// also waited out the lateness keeps every window one lateness-period too wide, inflating every
		// aggregate it publishes.
		let rolling = RollingOverTime::new(ms(5_000), ms(0));

		assert_eq!(rolling.eviction_cutoff(at_millis(8_000)), Some(at_millis(3_000)));
		assert_eq!(rolling.seal_horizon(at_millis(8_000), ms(200)), at_millis(2_800));
	}

	#[test]
	fn a_ledger_younger_than_the_span_evicts_nothing_rather_than_clamping_to_the_epoch() {
		// At startup the ledger sits near the epoch while the span is minutes. Underflowing yields a
		// cutoff near the maximum instant and evicts everything; clamping to the epoch is wrong too,
		// since eviction is inclusive and a row at the epoch could then never be retained.
		let rolling = RollingOverTime::new(ms(5_000), ms(0));

		assert_eq!(rolling.eviction_cutoff(at_millis(0)), None);
		assert_eq!(rolling.eviction_cutoff(at_millis(1_000)), None);
		assert_eq!(
			rolling.eviction_cutoff(at_millis(5_000)),
			Some(at_millis(0)),
			"a span that has exactly elapsed yields a real cutoff, not another None"
		);
		assert_eq!(rolling.seal_horizon(at_millis(1_000), ms(200)), at_millis(0));
	}

	#[test]
	fn the_seal_horizon_never_sits_later_than_the_eviction_cutoff() {
		// Sealing decides what may still be amended, eviction what is still stored. A horizon later
		// than the cutoff declares a window sealed while its rows are still retained, refusing a late
		// row the window could have accepted.
		for lag in [ms(0), ms(1), ms(9_000)] {
			for lateness in [ms(0), ms(1), ms(9_000)] {
				let rolling = RollingOverTime::new(ms(5_000), lag);
				let cutoff = rolling
					.eviction_cutoff(at_millis(60_000))
					.expect("a ledger well past the span must yield a cutoff");
				assert!(
					rolling.seal_horizon(at_millis(60_000), lateness) <= cutoff,
					"horizon passed the cutoff at lag {lag:?} lateness {lateness:?}"
				);
			}
		}
	}
}
