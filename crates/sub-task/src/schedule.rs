use std::time::{Duration, Instant};

/// Defines when and how often a task should be executed
#[derive(Debug, Clone)]
pub enum Schedule {
	/// Execute at fixed intervals (interval starts after task completion)
	FixedInterval(Duration),
	/// Execute once after a delay
	Once(Duration),
}

impl Schedule {
	/// Calculate the next execution time after the given instant
	/// Returns None for one-shot tasks
	pub fn next_execution(&self, after: Instant) -> Option<Instant> {
		match self {
			Schedule::FixedInterval(duration) => Some(after + *duration),
			Schedule::Once(_) => None,
		}
	}

	/// Get the initial delay for first execution
	pub fn initial_delay(&self) -> Duration {
		match self {
			Schedule::FixedInterval(duration) => *duration,
			Schedule::Once(delay) => *delay,
		}
	}

	/// Validate the schedule
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
	use super::*;

	#[test]
	fn test_fixed_interval_next_execution() {
		let schedule = Schedule::FixedInterval(Duration::from_secs(10));
		let now = Instant::now();
		let next = schedule.next_execution(now);
		assert!(next.is_some());
		assert_eq!(next.unwrap(), now + Duration::from_secs(10));
	}

	#[test]
	fn test_once_next_execution() {
		let schedule = Schedule::Once(Duration::from_secs(5));
		let now = Instant::now();
		let next = schedule.next_execution(now);
		assert!(next.is_none());
	}

	#[test]
	fn test_initial_delay() {
		let interval = Schedule::FixedInterval(Duration::from_secs(30));
		assert_eq!(interval.initial_delay(), Duration::from_secs(30));

		let once = Schedule::Once(Duration::from_secs(5));
		assert_eq!(once.initial_delay(), Duration::from_secs(5));
	}

	#[test]
	fn test_validation() {
		let valid = Schedule::FixedInterval(Duration::from_secs(1));
		assert!(valid.validate().is_ok());

		let invalid = Schedule::FixedInterval(Duration::ZERO);
		assert!(invalid.validate().is_err());
	}
}
