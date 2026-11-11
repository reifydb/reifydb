// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

use crate::{
	operator::state::seal::{coord::Coord, rule::SealRule},
	window::coord::DateTimeCoord,
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
		self.size.saturating_add(self.lag)
	}

	pub fn seal_rule(&self, lateness: Duration) -> SealRule {
		SealRule::rolling(self.span(), lateness)
	}

	pub fn eviction_cutoff(&self, ledger: DateTimeCoord) -> Option<DateTimeCoord> {
		ledger.checked_sub_span(self.span())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::factory::coord::event_coord_at_millis;

	fn ms(millis: u64) -> Duration {
		Duration::from_milliseconds_const(millis as i64)
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
	fn forget_uses_the_bare_span_and_sealing_adds_the_lateness() {
		// Rolling admits a late row inside the lateness but evicts on the bare span. An eviction that
		// also waited out the lateness keeps every window one lateness-period too wide, inflating every
		// aggregate it publishes.
		let rolling = RollingOverTime::new(ms(5_000), ms(0));

		assert_eq!(rolling.eviction_cutoff(event_coord_at_millis(8_000)), Some(event_coord_at_millis(3_000)));
	}

	#[test]
	fn a_ledger_younger_than_the_span_yields_a_real_pre_epoch_cutoff() {
		// Wrapping through u64 here would put the cutoff near the maximum instant and evict everything.
		let rolling = RollingOverTime::new(ms(5_000), ms(0));

		assert_eq!(rolling.eviction_cutoff(event_coord_at_millis(0)), Some(event_coord_at_millis(-5_000)));
		assert_eq!(rolling.eviction_cutoff(event_coord_at_millis(1_000)), Some(event_coord_at_millis(-4_000)));
		assert_eq!(
			rolling.eviction_cutoff(event_coord_at_millis(5_000)),
			Some(event_coord_at_millis(0)),
			"a span that has exactly elapsed yields a cutoff at the epoch"
		);
	}
}
