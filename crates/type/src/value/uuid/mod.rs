// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{
	cmp::Ordering,
	fmt::{Display, Formatter},
	ops::Deref,
};

use ::uuid::Uuid as StdUuid;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod parse;

pub use parse::{parse_uuid4, parse_uuid7};

/// A UUID version 4 (random) wrapper type
#[repr(transparent)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Uuid4(pub StdUuid);

impl Uuid4 {
	/// Generate a new random UUID v4
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
		debug_assert!(uuid.get_version_num() == 4 || uuid.get_version_num() == 0);
		Uuid4(uuid)
	}
}

impl From<Uuid4> for StdUuid {
	fn from(uuid4: Uuid4) -> Self {
		uuid4.0
	}
}

impl Display for Uuid4 {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

/// A UUID version 7 (timestamp-based) wrapper type
#[repr(transparent)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Uuid7(pub StdUuid);

impl Default for Uuid7 {
	fn default() -> Self {
		Self(Uuid::nil())
	}
}

impl Uuid7 {
	/// Generate a new timestamp-based UUID v7
	pub fn generate() -> Self {
		Uuid7(StdUuid::now_v7())
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
		debug_assert!(uuid.get_version_num() == 7 || uuid.get_version_num() == 0);
		Uuid7(uuid)
	}
}

impl From<Uuid7> for StdUuid {
	fn from(uuid7: Uuid7) -> Self {
		uuid7.0
	}
}

impl Display for Uuid7 {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
	use super::*;

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

		// Should be consistently ordered
		let cmp1 = uuid4_a.cmp(&uuid4_b);
		let cmp2 = uuid4_a.cmp(&uuid4_b);
		assert_eq!(cmp1, cmp2);

		// Self comparison should be Equal
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
		let uuid7 = Uuid7::generate();
		assert_eq!(uuid7.get_version_num(), 7);
	}

	#[test]
	fn test_uuid7_equality() {
		let std_uuid = StdUuid::now_v7();
		let uuid7_a = Uuid7(std_uuid);
		let uuid7_b = Uuid7(std_uuid);
		let uuid7_c = Uuid7::generate();

		assert_eq!(uuid7_a, uuid7_b);
		assert_ne!(uuid7_a, uuid7_c);
	}

	#[test]
	fn test_uuid7_ordering() {
		let uuid7_a = Uuid7::generate();
		let uuid7_b = Uuid7::generate();

		// Should be consistently ordered
		let cmp1 = uuid7_a.cmp(&uuid7_b);
		let cmp2 = uuid7_a.cmp(&uuid7_b);
		assert_eq!(cmp1, cmp2);

		// Self comparison should be Equal
		assert_eq!(uuid7_a.cmp(&uuid7_a), Ordering::Equal);
	}

	#[test]
	fn test_uuid7_display() {
		let std_uuid = StdUuid::now_v7();
		let uuid7 = Uuid7(std_uuid);

		assert_eq!(format!("{}", uuid7), format!("{}", std_uuid));
	}

	#[test]
	fn test_uuid7_timestamp_ordering() {
		// UUID v7 should have timestamp-based ordering for UUIDs
		// generated close in time
		let uuid7_first = Uuid7::generate();
		std::thread::sleep(std::time::Duration::from_millis(1));
		let uuid7_second = Uuid7::generate();

		// The first UUID should be less than the second (in most cases
		// due to timestamp) Note: This test might occasionally fail
		// due to timing, but it demonstrates the concept
		assert!(uuid7_first <= uuid7_second);
	}
}
