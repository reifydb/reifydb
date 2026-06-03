// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::context::clock::Instant;
use reifydb_value::value::duration::Duration;

#[derive(Debug, Clone)]
pub enum Schedule {
	FixedInterval(Duration),

	Once(Duration),
}

impl Schedule {
	pub fn next_execution(&self, after: Instant) -> Option<Instant> {
		match self {
			Schedule::FixedInterval(duration) => Some(after + duration.to_std()),
			Schedule::Once(_) => None,
		}
	}

	pub fn initial_delay(&self) -> Duration {
		match self {
			Schedule::FixedInterval(duration) => *duration,
			Schedule::Once(delay) => *delay,
		}
	}

	pub fn validate(&self) -> Result<(), String> {
		match self {
			Schedule::FixedInterval(duration) => {
				if duration.is_zero() {
					return Err("FixedInterval duration cannot be zero".to_string());
				}
				Ok(())
			}
			Schedule::Once(delay) => {
				if delay.is_zero() {
					return Err("Once delay cannot be zero".to_string());
				}
				Ok(())
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_runtime::context::clock::Clock;

	use super::*;

	#[test]
	fn test_fixed_interval_next_execution() {
		let clock = Clock::Real;
		let schedule = Schedule::FixedInterval(Duration::from_seconds(10).unwrap());
		let now = clock.instant();
		let next = schedule.next_execution(now.clone());
		assert!(next.is_some());
		assert_eq!(next.unwrap(), now + Duration::from_seconds(10).unwrap().to_std());
	}

	#[test]
	fn test_once_next_execution() {
		let clock = Clock::Real;
		let schedule = Schedule::Once(Duration::from_seconds(5).unwrap());
		let now = clock.instant();
		let next = schedule.next_execution(now);
		assert!(next.is_none());
	}

	#[test]
	fn test_initial_delay() {
		let interval = Schedule::FixedInterval(Duration::from_seconds(30).unwrap());
		assert_eq!(interval.initial_delay(), Duration::from_seconds(30).unwrap());

		let once = Schedule::Once(Duration::from_seconds(5).unwrap());
		assert_eq!(once.initial_delay(), Duration::from_seconds(5).unwrap());
	}

	#[test]
	fn test_validation() {
		let valid = Schedule::FixedInterval(Duration::from_seconds(1).unwrap());
		assert!(valid.validate().is_ok());

		let invalid = Schedule::FixedInterval(Duration::zero());
		assert!(invalid.validate().is_err());
	}
}
