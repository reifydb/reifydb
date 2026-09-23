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
		uint::Uint,
		value_type::ValueType,
	},
};

pub mod parse;

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Int(i256);

impl Int {
	pub const MAX: Int = Int(unscaled::MAX);
	pub const MIN: Int = Int(unscaled::MIN);

	pub fn from_i256(value: i256) -> Option<Self> {
		unscaled::checked(value).map(Self)
	}

	pub fn from_i64(value: i64) -> Self {
		Self(i256::from_i128(value as i128))
	}

	pub fn from_i128(value: i128) -> Self {
		Self(i256::from_i128(value))
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

	pub fn to_i128(&self) -> Option<i128> {
		self.0.to_i128()
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

	pub fn is_negative(&self) -> bool {
		self.0.is_negative()
	}

	pub fn abs(&self) -> Self {
		Self(self.0.wrapping_abs())
	}

	pub fn negate(&self) -> Self {
		Self(self.0.wrapping_neg())
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
		self.checked_add(other).unwrap_or(Self::saturated(other.is_negative()))
	}

	pub fn saturating_sub(&self, other: &Self) -> Self {
		self.checked_sub(other).unwrap_or(Self::saturated(!other.is_negative()))
	}

	pub fn saturating_mul(&self, other: &Self) -> Self {
		self.checked_mul(other).unwrap_or(Self::saturated(self.is_negative() != other.is_negative()))
	}

	fn saturated(negative: bool) -> Self {
		if negative {
			Self::MIN
		} else {
			Self::MAX
		}
	}
}

impl Display for Int {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.0, f)
	}
}

impl FromStr for Int {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::parse(s).map_err(|error| parse_error(error, Fragment::None))
	}
}

pub(crate) fn parse_error(error: ParseError, fragment: Fragment) -> Error {
	match error {
		ParseError::Invalid => TypeError::InvalidNumberFormat {
			target: ValueType::INT,
			fragment,
		}
		.into(),
		ParseError::OutOfRange => TypeError::NumberOutOfRange {
			target: ValueType::INT,
			fragment,
			descriptor: None,
		}
		.into(),
	}
}

macro_rules! int_from_signed {
	($($t:ty),*) => {
		$(impl From<$t> for Int {
			fn from(value: $t) -> Self {
				Self::from_i128(value as i128)
			}
		})*
	};
}

int_from_signed!(i8, i16, i32, i64, i128, u8, u16, u32, u64);

impl From<u128> for Int {
	fn from(value: u128) -> Self {
		Self::from_u128(value)
	}
}

impl From<Uint> for Int {
	fn from(value: Uint) -> Self {
		Self(value.to_i256())
	}
}

impl Serialize for Int {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.collect_str(self)
	}
}

struct IntVisitor;

impl<'de> Visitor<'de> for IntVisitor {
	type Value = Int;

	fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
		formatter.write_str("an integer as a string")
	}

	fn visit_str<E>(self, value: &str) -> Result<Int, E>
	where
		E: de::Error,
	{
		match Parsed::parse(value).and_then(|parsed| parsed.to_decimal()) {
			Ok((unscaled, 0)) => Ok(Int(unscaled)),
			_ => Err(E::custom(format!("invalid int {value:?}"))),
		}
	}
}

impl<'de> Deserialize<'de> for Int {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_str(IntVisitor)
	}
}

#[cfg(test)]
pub mod tests {
	use std::{cmp::Ordering, collections::HashSet};

	use super::*;

	#[test]
	fn test_int_create() {
		let int = Int::from_i64(42);
		assert_eq!(format!("{}", int), "42");
	}

	#[test]
	fn test_int_equality() {
		let a = Int::from_i64(100);
		let b = Int::from_i64(100);
		let c = Int::from_i64(200);

		assert_eq!(a, b);
		assert_ne!(a, c);
	}

	#[test]
	fn test_int_ordering() {
		let a = Int::from_i64(10);
		let b = Int::from_i64(20);
		let c = Int::from_i64(20);

		assert!(a < b);
		assert!(b > a);
		assert_eq!(b.cmp(&c), Ordering::Equal);
	}

	#[test]
	fn test_int_large_values() {
		let large = Int::from_i128(i128::MAX);
		let larger = Int::from_u128(i128::MAX as u128 + 1);

		assert!(large < larger);
	}

	#[test]
	fn test_int_display() {
		let int = Int::from_i64(-12345);
		assert_eq!(format!("{}", int), "-12345");
	}

	#[test]
	fn test_int_hash() {
		let a = Int::from_i64(42);
		let b = Int::from_i64(42);

		let mut set = HashSet::new();
		set.insert(a);
		assert!(set.contains(&b));
	}

	#[test]
	fn range_stops_below_ten_to_the_seventy_six() {
		// A 77 digit Int cannot be stored in any int column, so every checked op must refuse it.
		assert_eq!(Int::MAX.to_string(), "9".repeat(76));
		assert_eq!(Int::MIN.to_string(), format!("-{}", "9".repeat(76)));
		assert_eq!(Int::from_i256(Int::MAX.to_i256().wrapping_add(i256::ONE)), None);
		assert_eq!(Int::MAX.checked_add(&Int::one()), None);
		assert_eq!(Int::MIN.checked_sub(&Int::one()), None);
		assert_eq!(Int::MAX.checked_mul(&Int::from(10)), None);
		assert_eq!(Int::MAX.checked_sub(&Int::one()).unwrap().digits(), 76);
		assert_eq!(Int::MAX.saturating_add(&Int::one()), Int::MAX);
		assert_eq!(Int::MIN.saturating_mul(&Int::from(2)), Int::MIN);
		assert_eq!(Int::MIN.saturating_mul(&Int::from(-2)), Int::MAX);
	}

	#[test]
	fn division_truncates_toward_zero_and_refuses_zero() {
		// Floor division would give -4 for -7 / 2 and break the remainder sign rule.
		assert_eq!(Int::from(-7).checked_div(&Int::from(2)), Some(Int::from(-3)));
		assert_eq!(Int::from(-7).checked_rem(&Int::from(2)), Some(Int::from(-1)));
		assert_eq!(Int::from(7).checked_rem(&Int::from(-2)), Some(Int::from(1)));
		assert_eq!(Int::from(7).checked_div(&Int::zero()), None);
		assert_eq!(Int::from(7).checked_rem(&Int::zero()), None);
	}

	#[test]
	fn serde_round_trips_the_full_range() {
		// A lossy encoding would change the value of a 76 digit int on every save.
		for value in [Int::MAX, Int::MIN, Int::zero(), Int::from(-42)] {
			let back: Int = postcard::from_bytes(&postcard::to_stdvec(&value).unwrap()).unwrap();
			assert_eq!(back, value);
			let back: Int = serde_json::from_str(&serde_json::to_string(&value).unwrap()).unwrap();
			assert_eq!(back, value);
		}
		assert!(serde_json::from_str::<Int>(&format!("\"{}\"", "9".repeat(77))).is_err());
		assert!(serde_json::from_str::<Int>("\"1.5\"").is_err());
	}

	#[test]
	fn float_conversion_truncates_and_refuses_what_does_not_fit() {
		// Rounding 2.9 to 3 would disagree with every other float to int cast.
		assert_eq!(Int::from_f64(2.9), Some(Int::from(2)));
		assert_eq!(Int::from_f64(-2.9), Some(Int::from(-2)));
		assert_eq!(Int::from_f64(1e76), None);
		assert_eq!(Int::from_f64(f64::NAN), None);
		assert_eq!(Int::from_f64(f64::INFINITY), None);
		assert_eq!(Int::MAX.to_f64(), 1e76);
		assert_eq!(Int::from(-3).to_f64(), -3.0);
	}
}
