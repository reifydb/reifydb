// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

use crate::operator::state::seal::rule::SealRule;

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
}

#[cfg(test)]
mod tests {
	use super::*;

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
}
