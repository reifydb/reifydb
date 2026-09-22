// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	fmt::{Display, Formatter},
	num::ParseIntError,
	str::FromStr,
};

use reifydb_value::value::duration::Duration;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Visitor};

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct CommitVersion(pub u64);

impl FromStr for CommitVersion {
	type Err = ParseIntError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Ok(CommitVersion(u64::from_str(s)?))
	}
}

impl Display for CommitVersion {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl PartialEq<i32> for CommitVersion {
	fn eq(&self, other: &i32) -> bool {
		self.0 == *other as u64
	}
}

impl PartialEq<CommitVersion> for i32 {
	fn eq(&self, other: &CommitVersion) -> bool {
		*self as u64 == other.0
	}
}

impl PartialEq<u64> for CommitVersion {
	fn eq(&self, other: &u64) -> bool {
		self.0.eq(other)
	}
}

impl From<CommitVersion> for u64 {
	fn from(value: CommitVersion) -> Self {
		value.0
	}
}

impl From<i32> for CommitVersion {
	fn from(value: i32) -> Self {
		Self(value as u64)
	}
}

impl From<u64> for CommitVersion {
	fn from(value: u64) -> Self {
		Self(value)
	}
}

impl Serialize for CommitVersion {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_u64(self.0)
	}
}

impl<'de> Deserialize<'de> for CommitVersion {
	fn deserialize<D>(deserializer: D) -> Result<CommitVersion, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct U64Visitor;

		impl Visitor<'_> for U64Visitor {
			type Value = CommitVersion;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("an unsigned 64-bit number")
			}

			fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
				Ok(CommitVersion(value))
			}
		}

		deserializer.deserialize_u64(U64Visitor)
	}
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceVersion(pub u64);

impl From<CommitVersion> for SourceVersion {
	fn from(version: CommitVersion) -> Self {
		Self(version.0)
	}
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChangeVersion {
	pub commit: CommitVersion,
	pub source: SourceVersion,
}

impl From<CommitVersion> for ChangeVersion {
	fn from(commit: CommitVersion) -> Self {
		Self {
			commit,
			source: SourceVersion::from(commit),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
pub enum JoinType {
	Inner,
	#[default]
	Left,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum IndexType {
	#[default]
	Index,
	Unique,
	Primary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowSize {
	Duration(Duration),
	Count(u64),
}

impl WindowSize {
	pub fn is_count(&self) -> bool {
		matches!(self, WindowSize::Count(_))
	}

	pub fn as_duration(&self) -> Option<Duration> {
		match self {
			WindowSize::Duration(d) => Some(*d),
			_ => None,
		}
	}

	pub fn as_count(&self) -> Option<u64> {
		match self {
			WindowSize::Count(c) => Some(*c),
			_ => None,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeDomain {
	None,
	Event,
	Processing,
}

impl TimeDomain {
	pub fn to_u8(self) -> u8 {
		match self {
			TimeDomain::None => 0,
			TimeDomain::Event => 1,
			TimeDomain::Processing => 2,
		}
	}

	pub fn from_u8(value: u8) -> Self {
		match value {
			1 => TimeDomain::Event,
			2 => TimeDomain::Processing,
			_ => TimeDomain::None,
		}
	}

	pub fn as_str(&self) -> &'static str {
		match self {
			TimeDomain::None => "none",
			TimeDomain::Event => "event",
			TimeDomain::Processing => "processing",
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorClass {
	Managed,
	Unmanaged,
	Nostate,
	Windowed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowSizeDomain {
	Time,
	Slots,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowRequirements {
	pub takes_window: bool,
	pub kinds: &'static [&'static str],
	pub domain: WindowSizeDomain,
	pub needs_pane: bool,
}

impl OperatorClass {
	pub fn to_u8(self) -> u8 {
		match self {
			OperatorClass::Managed => 1,
			OperatorClass::Unmanaged => 2,
			OperatorClass::Nostate => 3,
			OperatorClass::Windowed => 4,
		}
	}

	pub fn from_u8(value: u8) -> Option<Self> {
		match value {
			1 => Some(OperatorClass::Managed),
			2 => Some(OperatorClass::Unmanaged),
			3 => Some(OperatorClass::Nostate),
			4 => Some(OperatorClass::Windowed),
			_ => None,
		}
	}
}

impl WindowSizeDomain {
	pub fn to_u8(self) -> u8 {
		match self {
			WindowSizeDomain::Time => 1,
			WindowSizeDomain::Slots => 2,
		}
	}

	pub fn from_u8(value: u8) -> Option<Self> {
		match value {
			1 => Some(WindowSizeDomain::Time),
			2 => Some(WindowSizeDomain::Slots),
			_ => None,
		}
	}
}

const WINDOW_KIND_NAMES: [&str; 4] = ["tumbling", "sliding", "session", "rolling"];

const WINDOW_KIND_SETS: [&[&str]; 16] = [
	&[],
	&["tumbling"],
	&["sliding"],
	&["tumbling", "sliding"],
	&["session"],
	&["tumbling", "session"],
	&["sliding", "session"],
	&["tumbling", "sliding", "session"],
	&["rolling"],
	&["tumbling", "rolling"],
	&["sliding", "rolling"],
	&["tumbling", "sliding", "rolling"],
	&["session", "rolling"],
	&["tumbling", "session", "rolling"],
	&["sliding", "session", "rolling"],
	&["tumbling", "sliding", "session", "rolling"],
];

impl WindowRequirements {
	pub fn kinds_bitmask(&self) -> Option<u32> {
		self.kinds.iter().try_fold(0u32, |mask, kind| {
			WINDOW_KIND_NAMES.iter().position(|name| name == kind).map(|bit| mask | 1 << bit)
		})
	}

	pub fn kinds_from_bitmask(mask: u32) -> Option<&'static [&'static str]> {
		WINDOW_KIND_SETS.get(usize::try_from(mask).ok()?).copied()
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeSource {
	None,
	Event {
		ts: String,
	},
	Processing,
}

impl TimeSource {
	pub fn domain(&self) -> TimeDomain {
		match self {
			TimeSource::None => TimeDomain::None,
			TimeSource::Event {
				..
			} => TimeDomain::Event,
			TimeSource::Processing => TimeDomain::Processing,
		}
	}

	pub fn ts(&self) -> Option<&str> {
		match self {
			TimeSource::Event {
				ts,
			} => Some(ts.as_str()),
			TimeSource::None | TimeSource::Processing => None,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowKind {
	Tumbling {
		size: WindowSize,
	},

	Sliding {
		size: WindowSize,
		slide: WindowSize,
	},

	Rolling {
		size: WindowSize,
		#[serde(default)]
		lag: Option<Duration>,
		#[serde(default)]
		pane: Option<Duration>,
	},

	Session {
		gap: Duration,
	},
}

impl WindowKind {
	pub fn size(&self) -> Option<&WindowSize> {
		match self {
			WindowKind::Tumbling {
				size,
				..
			} => Some(size),
			WindowKind::Sliding {
				size,
				..
			} => Some(size),
			WindowKind::Rolling {
				size,
				..
			} => Some(size),
			WindowKind::Session {
				..
			} => None,
		}
	}

	pub fn name(&self) -> &'static str {
		match self {
			WindowKind::Tumbling {
				..
			} => "tumbling",
			WindowKind::Sliding {
				..
			} => "sliding",
			WindowKind::Rolling {
				..
			} => "rolling",
			WindowKind::Session {
				..
			} => "session",
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn every_operator_class_round_trips_through_its_byte_and_zero_is_refused() {
		// a zeroed or foreign class byte read as a real class would admit a guest to the wrong keyspaces
		for class in [
			OperatorClass::Managed,
			OperatorClass::Unmanaged,
			OperatorClass::Nostate,
			OperatorClass::Windowed,
		] {
			assert_eq!(OperatorClass::from_u8(class.to_u8()), Some(class));
		}
		assert_eq!(OperatorClass::from_u8(0), None);
		assert_eq!(OperatorClass::from_u8(5), None);
	}

	#[test]
	fn every_window_size_domain_round_trips_through_its_byte_and_zero_is_refused() {
		// a domain read backwards fires FLOW_068 and FLOW_069 on exactly the views that are correct
		for domain in [WindowSizeDomain::Time, WindowSizeDomain::Slots] {
			assert_eq!(WindowSizeDomain::from_u8(domain.to_u8()), Some(domain));
		}
		assert_eq!(WindowSizeDomain::from_u8(0), None);
		assert_eq!(WindowSizeDomain::from_u8(3), None);
	}

	fn requirements(kinds: &'static [&'static str]) -> WindowRequirements {
		WindowRequirements {
			takes_window: true,
			kinds,
			domain: WindowSizeDomain::Time,
			needs_pane: false,
		}
	}

	#[test]
	fn every_published_kind_list_survives_the_bitmask() {
		// a swapped bit would let CREATE accept a kind the driver cannot run and refuse one it can
		for kinds in [
			&["tumbling"][..],
			&["rolling"],
			&["tumbling", "sliding"],
			&["tumbling", "sliding", "rolling"],
			&["tumbling", "sliding", "session"],
			&["tumbling", "sliding", "session", "rolling"],
			&[],
		] {
			let mask = requirements(kinds).kinds_bitmask().unwrap();
			assert_eq!(WindowRequirements::kinds_from_bitmask(mask), Some(kinds));
		}
	}

	#[test]
	fn each_kind_owns_exactly_one_bit() {
		// two kinds sharing a bit would make the host unable to tell them apart
		let bits: Vec<u32> = [&["tumbling"][..], &["sliding"], &["session"], &["rolling"]]
			.into_iter()
			.map(|kinds| requirements(kinds).kinds_bitmask().unwrap())
			.collect();
		assert_eq!(bits, vec![1, 2, 4, 8]);
	}

	#[test]
	fn an_unknown_kind_name_or_bit_is_refused() {
		// a mask the host cannot map back must fail the load, not shrink to the kinds it knows
		assert_eq!(requirements(&["hopping"]).kinds_bitmask(), None);
		assert_eq!(requirements(&["tumbling", "hopping"]).kinds_bitmask(), None);
		assert_eq!(WindowRequirements::kinds_from_bitmask(16), None);
		assert_eq!(WindowRequirements::kinds_from_bitmask(u32::MAX), None);
	}
}
