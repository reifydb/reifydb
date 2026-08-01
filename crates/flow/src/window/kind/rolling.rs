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

	pub fn eviction_cutoff(&self, ledger: DateTime) -> Option<DateTime> {
		ledger.checked_sub_span(self.span())
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
		// A rolling window over ROWS always has a current value, so there is no lag, grace or seal
		// instant to ask for. Lag and grace are milliseconds, and subtracting them from a row number
		// would drop rows in proportion to the lag; the type carries no such method to answer wrongly.
		let rows = RollingOverRows::new(64);

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
	fn eviction_uses_the_bare_span_and_sealing_adds_the_grace() {
		// Rolling admits a late row inside the grace but evicts on the bare span. An eviction that
		// also waited out the grace keeps every window one grace-period too wide, inflating every
		// aggregate it publishes.
		let rolling = RollingOverTime::new(ms(5_000), ms(0));

		assert_eq!(rolling.eviction_cutoff(at(8_000)), Some(at(3_000)));
		assert_eq!(rolling.seal_horizon(at(8_000), ms(200)), at(2_800));
	}

	#[test]
	fn a_ledger_younger_than_the_span_evicts_nothing_rather_than_clamping_to_the_epoch() {
		// At startup the ledger sits near the epoch while the span is minutes. Underflowing yields a
		// cutoff near the maximum instant and evicts everything; clamping to the epoch is wrong too,
		// since eviction is inclusive and a row at the epoch could then never be retained.
		let rolling = RollingOverTime::new(ms(5_000), ms(0));

		assert_eq!(rolling.eviction_cutoff(at(0)), None);
		assert_eq!(rolling.eviction_cutoff(at(1_000)), None);
		assert_eq!(
			rolling.eviction_cutoff(at(5_000)),
			Some(at(0)),
			"a span that has exactly elapsed yields a real cutoff, not another None"
		);
		assert_eq!(rolling.seal_horizon(at(1_000), ms(200)), at(0));
	}

	#[test]
	fn the_seal_horizon_never_sits_later_than_the_eviction_cutoff() {
		// Sealing decides what may still be amended, eviction what is still stored. A horizon later
		// than the cutoff declares a window sealed while its rows are still retained, refusing a late
		// row the window could have accepted.
		for lag in [ms(0), ms(1), ms(9_000)] {
			for grace in [ms(0), ms(1), ms(9_000)] {
				let rolling = RollingOverTime::new(ms(5_000), lag);
				let cutoff = rolling
					.eviction_cutoff(at(60_000))
					.expect("a ledger well past the span must yield a cutoff");
				assert!(
					rolling.seal_horizon(at(60_000), grace) <= cutoff,
					"horizon passed the cutoff at lag {lag:?} grace {grace:?}"
				);
			}
		}
	}
}
