// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	fmt,
	fmt::{Display, Formatter},
	ops::Deref,
};

use ::uuid::{Builder, Uuid as StdUuid};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
	clock::{ClockNow, RandomBytes},
	reifydb_assertions,
};

pub mod parse;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Uuid4(pub StdUuid);

impl Uuid4 {
	pub fn generate() -> Self {
		Uuid4(StdUuid::new_v4())
	}
}

impl Default for Uuid4 {
	fn default() -> Self {
		Self(Uuid::nil())
	}
}

impl Deref for Uuid4 {
	type Target = StdUuid;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialOrd for Uuid4 {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Uuid4 {
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.as_bytes().cmp(other.0.as_bytes())
	}
}

impl From<StdUuid> for Uuid4 {
	fn from(uuid: StdUuid) -> Self {
		reifydb_assertions! {
			assert!(uuid.get_version_num() == 4 || uuid.get_version_num() == 0);
		}
		Uuid4(uuid)
	}
}

impl From<Uuid4> for StdUuid {
	fn from(uuid4: Uuid4) -> Self {
		uuid4.0
	}
}

impl Display for Uuid4 {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Uuid7(pub StdUuid);

impl Default for Uuid7 {
	fn default() -> Self {
		Self(Uuid::nil())
	}
}

impl Uuid7 {
	pub fn generate<C: ClockNow, R: RandomBytes>(clock: &C, rng: &R) -> Self {
		let millis = clock.now_millis();
		let random_bytes = rng.bytes_10();
		Uuid7(Builder::from_unix_timestamp_millis(millis, &random_bytes).into_uuid())
	}
}

impl Deref for Uuid7 {
	type Target = StdUuid;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl PartialOrd for Uuid7 {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Uuid7 {
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.as_bytes().cmp(other.0.as_bytes())
	}
}

impl From<StdUuid> for Uuid7 {
	fn from(uuid: StdUuid) -> Self {
		reifydb_assertions! {
			assert!(uuid.get_version_num() == 7 || uuid.get_version_num() == 0);
		}
		Uuid7(uuid)
	}
}

impl From<Uuid7> for StdUuid {
	fn from(uuid7: Uuid7) -> Self {
		uuid7.0
	}
}

impl Display for Uuid7 {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
pub mod tests {
	use super::*;
	use crate::clock::testing::{TestClock, TestRng};

	fn test_clock_and_rng() -> (TestClock, TestClock, TestRng) {
		let clock = TestClock::from_millis(1000);
		(clock.clone(), clock, TestRng)
	}

	#[test]
	fn test_uuid4_generate() {
		let uuid4 = Uuid4::generate();
		assert_eq!(uuid4.get_version_num(), 4);
	}

	#[test]
	fn test_uuid4_equality() {
		let std_uuid = StdUuid::new_v4();
		let uuid4_a = Uuid4(std_uuid);
		let uuid4_b = Uuid4(std_uuid);
		let uuid4_c = Uuid4::generate();

		assert_eq!(uuid4_a, uuid4_b);
		assert_ne!(uuid4_a, uuid4_c);
	}

	#[test]
	fn test_uuid4_ordering() {
		let uuid4_a = Uuid4::generate();
		let uuid4_b = Uuid4::generate();

		let cmp1 = uuid4_a.cmp(&uuid4_b);
		let cmp2 = uuid4_a.cmp(&uuid4_b);
		assert_eq!(cmp1, cmp2);

		assert_eq!(uuid4_a.cmp(&uuid4_a), Ordering::Equal);
	}

	#[test]
	fn test_uuid4_display() {
		let std_uuid = StdUuid::new_v4();
		let uuid4 = Uuid4(std_uuid);

		assert_eq!(format!("{}", uuid4), format!("{}", std_uuid));
	}

	#[test]
	fn test_uuid7_generate() {
		let (_, clock, rng) = test_clock_and_rng();
		let uuid7 = Uuid7::generate(&clock, &rng);
		assert_eq!(uuid7.get_version_num(), 7);
	}

	#[test]
	fn test_uuid7_equality() {
		let (mock, clock, rng) = test_clock_and_rng();
		let uuid7_a = Uuid7::generate(&clock, &rng);
		let uuid7_b = Uuid7(uuid7_a.0);
		mock.advance_millis(1);
		let uuid7_c = Uuid7::generate(&clock, &rng);

		assert_eq!(uuid7_a, uuid7_b);
		assert_ne!(uuid7_a, uuid7_c);
	}

	#[test]
	fn test_uuid7_ordering() {
		let (mock, clock, rng) = test_clock_and_rng();
		let uuid7_a = Uuid7::generate(&clock, &rng);
		mock.advance_millis(1);
		let uuid7_b = Uuid7::generate(&clock, &rng);

		let cmp1 = uuid7_a.cmp(&uuid7_b);
		let cmp2 = uuid7_a.cmp(&uuid7_b);
		assert_eq!(cmp1, cmp2);

		assert_eq!(uuid7_a.cmp(&uuid7_a), Ordering::Equal);
	}

	#[test]
	fn test_uuid7_display() {
		let (_, clock, rng) = test_clock_and_rng();
		let uuid7 = Uuid7::generate(&clock, &rng);
		let display = format!("{}", uuid7);
		assert!(!display.is_empty());
	}

	#[test]
	fn test_uuid7_timestamp_ordering() {
		let (mock, clock, rng) = test_clock_and_rng();
		let uuid7_first = Uuid7::generate(&clock, &rng);
		mock.advance_millis(1);
		let uuid7_second = Uuid7::generate(&clock, &rng);

		assert!(uuid7_first < uuid7_second);
	}
}
