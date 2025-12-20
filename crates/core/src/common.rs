use std::{
	fmt,
	fmt::{Display, Formatter},
	num::ParseIntError,
	str::FromStr,
	time::Duration,
};

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

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
	Inner,
	Left,
}

impl Default for JoinType {
	fn default() -> Self {
		JoinType::Left
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
	Index,
	Unique,
	Primary,
}

impl Default for IndexType {
	fn default() -> Self {
		IndexType::Index
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowType {
	Time(WindowTimeMode),
	Count,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowTimeMode {
	Processing,
	EventTime(String),
}

impl Default for WindowType {
	fn default() -> Self {
		WindowType::Time(WindowTimeMode::Processing)
	}
}

impl Default for WindowTimeMode {
	fn default() -> Self {
		WindowTimeMode::Processing
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowSize {
	Duration(Duration),
	Count(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowSlide {
	Duration(Duration),
	Count(u64),
	Rolling,
}
