// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	fmt::{Display, Formatter},
	str::FromStr,
};

use arrow_buffer::i256;
use num_traits::ToPrimitive;
use serde::{
	Deserialize, Deserializer, Serialize, Serializer,
	de::{self, Visitor},
};

use crate::{
	error::{Error, TypeError},
	fragment::Fragment,
	value::{
		decimal::unscaled::{self, ParseError, Parsed},
		value_type::ValueType,
	},
};

pub mod parse;

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Uint(i256);

impl Uint {
	pub const MAX: Uint = Uint(unscaled::MAX);

	pub fn from_i256(value: i256) -> Option<Self> {
		if value.is_negative() {
			return None;
		}
		unscaled::checked(value).map(Self)
	}

	pub fn from_u64(value: u64) -> Self {
		Self(i256::from_i128(value as i128))
	}

	pub fn from_u128(value: u128) -> Self {
		Self(i256::from_parts(value, 0))
	}

	pub fn from_f64(value: f64) -> Option<Self> {
		Self::from_i256(i256::from_f64(value.trunc())?)
	}

	pub fn parse(text: &str) -> Result<Self, ParseError> {
		Self::from_i256(Parsed::parse(text)?.to_integer()?).ok_or(ParseError::OutOfRange)
	}

	pub fn zero() -> Self {
		Self(i256::ZERO)
	}

	pub fn one() -> Self {
		Self(i256::ONE)
	}

	pub fn to_i256(&self) -> i256 {
		self.0
	}

	pub fn to_u128(&self) -> Option<u128> {
		let (low, high) = self.0.to_parts();
		(high == 0).then_some(low)
	}

	pub fn to_f64(&self) -> f64 {
		self.0.to_f64().expect("every i256 converts to f64")
	}

	pub fn digits(&self) -> u8 {
		unscaled::digits(self.0)
	}

	pub fn is_zero(&self) -> bool {
		self.0 == i256::ZERO
	}

	pub fn checked_add(&self, other: &Self) -> Option<Self> {
		Self::from_i256(self.0.checked_add(other.0)?)
	}

	pub fn checked_sub(&self, other: &Self) -> Option<Self> {
		Self::from_i256(self.0.checked_sub(other.0)?)
	}

	pub fn checked_mul(&self, other: &Self) -> Option<Self> {
		Self::from_i256(self.0.checked_mul(other.0)?)
	}

	pub fn checked_div(&self, other: &Self) -> Option<Self> {
		Self::from_i256(self.0.checked_div(other.0)?)
	}

	pub fn checked_rem(&self, other: &Self) -> Option<Self> {
		Self::from_i256(self.0.checked_rem(other.0)?)
	}

	pub fn saturating_add(&self, other: &Self) -> Self {
		self.checked_add(other).unwrap_or(Self::MAX)
	}

	pub fn saturating_sub(&self, other: &Self) -> Self {
		self.checked_sub(other).unwrap_or_default()
	}

	pub fn saturating_mul(&self, other: &Self) -> Self {
		self.checked_mul(other).unwrap_or(Self::MAX)
	}
}

impl Display for Uint {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl FromStr for Uint {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::parse(s).map_err(|error| parse_error(error, Fragment::None))
	}
}

pub(crate) fn parse_error(error: ParseError, fragment: Fragment) -> Error {
	match error {
		ParseError::Invalid => TypeError::InvalidNumberFormat {
			target: ValueType::UINT,
			fragment,
		}
		.into(),
		ParseError::OutOfRange => TypeError::NumberOutOfRange {
			target: ValueType::UINT,
			fragment,
			descriptor: None,
		}
		.into(),
	}
}

macro_rules! uint_from_unsigned {
	($($t:ty),*) => {
		$(impl From<$t> for Uint {
			fn from(value: $t) -> Self {
				Self::from_u128(value as u128)
			}
		})*
	};
}

uint_from_unsigned!(u8, u16, u32, u64, u128);

macro_rules! uint_from_signed {
	($($t:ty),*) => {
		$(impl From<$t> for Uint {
			fn from(value: $t) -> Self {
				Self::from_u128(value.max(0) as u128)
			}
		})*
	};
}

uint_from_signed!(i8, i16, i32, i64, i128);

impl Serialize for Uint {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.collect_str(self)
	}
}

struct UintVisitor;

impl<'de> Visitor<'de> for UintVisitor {
	type Value = Uint;

	fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
		formatter.write_str("an unsigned integer as a string")
	}

	fn visit_str<E>(self, value: &str) -> Result<Uint, E>
	where
		E: de::Error,
	{
		match Parsed::parse(value).and_then(|parsed| parsed.to_decimal()) {
			Ok((unscaled, 0)) if !unscaled.is_negative() => Ok(Uint(unscaled)),
			_ => Err(E::custom(format!("invalid uint {value:?}"))),
		}
	}
}

impl<'de> Deserialize<'de> for Uint {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_str(UintVisitor)
	}
}

#[cfg(test)]
pub mod tests {
	use std::{cmp::Ordering, collections::HashSet};

	use postcard::{from_bytes, to_stdvec};
	use serde_json::from_str;

	use super::*;

	#[test]
	fn test_uint_create() {
		let uint = Uint::from_u64(42);
		assert_eq!(format!("{}", uint), "42");
	}

	#[test]
	fn test_uint_equality() {
		let a = Uint::from_u64(100);
		let b = Uint::from_u64(100);
		let c = Uint::from_u64(200);

		assert_eq!(a, b);
		assert_ne!(a, c);
	}

	#[test]
	fn test_uint_ordering() {
		let a = Uint::from_u64(10);
		let b = Uint::from_u64(20);
		let c = Uint::from_u64(20);

		assert!(a < b);
		assert!(b > a);
		assert_eq!(b.cmp(&c), Ordering::Equal);
	}

	#[test]
	fn test_uint_large_values() {
		let large = Uint::from_u128(u128::MAX);
		let larger = Uint::from_i256(i256::from_parts(0, 1)).unwrap();

		assert!(large < larger);
	}

	#[test]
	fn test_uint_display() {
		let uint = Uint::from_u64(12345);
		assert_eq!(format!("{}", uint), "12345");
	}

	#[test]
	fn test_uint_hash() {
		let a = Uint::from_u64(42);
		let b = Uint::from_u64(42);

		let mut set = HashSet::new();
		set.insert(a);
		assert!(set.contains(&b));
	}

	#[test]
	fn test_uint_negative_input() {
		// A negative primitive clamps to zero instead of wrapping to a huge unsigned value.
		let negative_i32 = Uint::from(-42i32);
		let negative_i64 = Uint::from(-999i64);
		let negative_i128 = Uint::from(-12345i128);

		assert_eq!(negative_i32, Uint::zero());
		assert_eq!(negative_i64, Uint::zero());
		assert_eq!(negative_i128, Uint::zero());

		let positive_i32 = Uint::from(42i32);
		let positive_i64 = Uint::from(999i64);
		assert_eq!(format!("{}", positive_i32), "42");
		assert_eq!(format!("{}", positive_i64), "999");
	}

	#[test]
	fn range_is_zero_to_seventy_six_nines() {
		// A negative or 77 digit Uint cannot be stored in a uint column, so checked ops must refuse both.
		assert_eq!(Uint::MAX.to_string(), "9".repeat(76));
		assert_eq!(Uint::from_i256(i256::MINUS_ONE), None);
		assert_eq!(Uint::from_i256(Uint::MAX.to_i256().wrapping_add(i256::ONE)), None);
		assert_eq!(Uint::zero().checked_sub(&Uint::one()), None);
		assert_eq!(Uint::from(3u8).checked_sub(&Uint::from(5u8)), None);
		assert_eq!(Uint::MAX.checked_add(&Uint::one()), None);
		assert_eq!(Uint::MAX.checked_mul(&Uint::from(2u8)), None);
		assert_eq!(Uint::from(5u8).checked_sub(&Uint::from(3u8)), Some(Uint::from(2u8)));
		assert_eq!(Uint::zero().saturating_sub(&Uint::one()), Uint::zero());
		assert_eq!(Uint::MAX.saturating_add(&Uint::one()), Uint::MAX);
		assert_eq!(Uint::from(7u8).checked_div(&Uint::zero()), None);
		assert_eq!(Uint::from_f64(-1.0), None);
		assert_eq!(Uint::from_f64(-0.5), Some(Uint::zero()));
	}

	#[test]
	fn to_u128_refuses_values_past_u128() {
		// Truncating to the low 128 bits would silently wrap a wide Uint.
		assert_eq!(Uint::from_u128(u128::MAX).to_u128(), Some(u128::MAX));
		assert_eq!(Uint::from_i256(i256::from_parts(0, 1)).unwrap().to_u128(), None);
	}

	#[test]
	fn serde_round_trips_and_refuses_negative_text() {
		// A negative uint read back from storage would break the non-negative invariant.
		for value in [Uint::MAX, Uint::zero(), Uint::from(42u8)] {
			let back: Uint = from_bytes(&to_stdvec(&value).unwrap()).unwrap();
			assert_eq!(back, value);
		}
		assert!(from_str::<Uint>("\"-1\"").is_err());
		assert!(from_str::<Uint>(&format!("\"{}\"", "9".repeat(77))).is_err());
	}
}
