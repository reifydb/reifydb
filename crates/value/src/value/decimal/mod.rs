// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	fmt,
	fmt::{Display, Formatter},
	hash,
	str::FromStr,
};

use arrow_buffer::i256;
use serde::{
	Deserialize, Deserializer, Serialize, Serializer,
	de::{self, Visitor},
};

use super::{int::Int, uint::Uint};
use crate::{
	error::{Error, TypeError},
	fragment::Fragment,
	value::value_type::ValueType,
};

pub mod parse;
pub mod unscaled;

use unscaled::{MAX_DIGITS, ParseError, Parsed};

#[derive(Clone, Debug, Default)]
pub struct Decimal {
	unscaled: i256,
	scale: u8,
}

impl Decimal {
	pub const MAX_SCALE: u8 = MAX_DIGITS;

	pub fn from_parts(unscaled: i256, scale: u8) -> Option<Self> {
		(scale <= MAX_DIGITS && unscaled::in_range(unscaled)).then_some(Self {
			unscaled,
			scale,
		})
	}

	pub fn parse(text: &str) -> Result<Self, ParseError> {
		let (unscaled, scale) = Parsed::parse(text)?.to_decimal()?;
		Ok(Self {
			unscaled,
			scale,
		})
	}

	pub fn zero() -> Self {
		Self::default()
	}

	pub fn one() -> Self {
		Self::from_i64(1)
	}

	pub fn from_i64(value: i64) -> Self {
		Self::integer(i256::from_i128(value as i128))
	}

	pub fn from_i128(value: i128) -> Self {
		Self::integer(i256::from_i128(value))
	}

	pub fn from_u128(value: u128) -> Self {
		Self::integer(i256::from_parts(value, 0))
	}

	pub fn from_f64(value: f64) -> Option<Self> {
		if !value.is_finite() {
			return None;
		}
		Self::from_float_text(&format!("{value:e}"))
	}

	pub fn from_f32(value: f32) -> Option<Self> {
		if !value.is_finite() {
			return None;
		}
		Self::from_float_text(&format!("{value:e}"))
	}

	pub fn saturated(negative: bool, scale: u8) -> Self {
		Self {
			unscaled: if negative {
				unscaled::MIN
			} else {
				unscaled::MAX
			},
			scale: scale.min(MAX_DIGITS),
		}
	}

	pub fn unscaled(&self) -> i256 {
		self.unscaled
	}

	pub fn scale(&self) -> u8 {
		self.scale
	}

	pub fn digits(&self) -> u8 {
		unscaled::digits(self.unscaled)
	}

	pub fn is_zero(&self) -> bool {
		self.unscaled == i256::ZERO
	}

	pub fn is_negative(&self) -> bool {
		self.unscaled.is_negative()
	}

	pub fn negate(&self) -> Self {
		Self {
			unscaled: self.unscaled.wrapping_neg(),
			scale: self.scale,
		}
	}

	pub fn abs(&self) -> Self {
		Self {
			unscaled: self.unscaled.wrapping_abs(),
			scale: self.scale,
		}
	}

	pub fn trunc(&self) -> i256 {
		match unscaled::pow10(self.scale) {
			Some(divisor) => self.unscaled.wrapping_div(divisor),
			None => i256::ZERO,
		}
	}

	pub fn to_f64(&self) -> f64 {
		self.exponent_text().parse().expect("a decimal exponent text always parses as f64")
	}

	pub fn to_f32(&self) -> f32 {
		self.exponent_text().parse().expect("a decimal exponent text always parses as f32")
	}

	pub fn rescale(&self, scale: u8) -> Option<Self> {
		if scale >= self.scale {
			let unscaled = unscaled::upscale(self.unscaled, scale - self.scale)?;
			return Self::from_parts(unscaled, scale);
		}
		let divisor = unscaled::pow10(self.scale - scale)?;
		if self.unscaled.wrapping_rem(divisor) != i256::ZERO {
			return None;
		}
		Self::from_parts(self.unscaled.wrapping_div(divisor), scale)
	}

	pub fn round_to_scale(&self, scale: u8) -> Option<Self> {
		if scale >= self.scale {
			return self.rescale(scale);
		}
		Self::from_parts(unscaled::round_half_up(self.unscaled, self.scale - scale), scale)
	}

	pub fn fits(&self, precision: u8, scale: u8) -> Option<Self> {
		let rescaled = self.rescale(scale)?;
		(rescaled.digits() <= precision).then_some(rescaled)
	}

	pub fn checked_add(&self, other: &Self) -> Option<Self> {
		let scale = self.scale.max(other.scale);
		let left = unscaled::upscale(self.unscaled, scale - self.scale)?;
		let right = unscaled::upscale(other.unscaled, scale - other.scale)?;
		Self::from_parts(left.checked_add(right)?, scale)
	}

	pub fn checked_sub(&self, other: &Self) -> Option<Self> {
		self.checked_add(&other.negate())
	}

	pub fn checked_mul(&self, other: &Self) -> Option<Self> {
		let scale = self.scale.checked_add(other.scale)?;
		if let Some(product) = self.unscaled.checked_mul(other.unscaled).and_then(|unscaled| Self::from_parts(unscaled, scale)) {
			return Some(product);
		}
		let ((left, left_scale), (right, right_scale)) = (self.normalized(), other.normalized());
		Self::from_parts(left.checked_mul(right)?, left_scale + right_scale)
	}

	pub fn checked_div(&self, other: &Self) -> Option<Self> {
		if other.is_zero() {
			return None;
		}
		let scale = self.scale.max(other.scale).max(6);
		let extra = u32::from(scale - self.scale) + u32::from(other.scale);
		let magnitude =
			unscaled::divide_half_up(self.unscaled.wrapping_abs(), other.unscaled.wrapping_abs(), extra)?;
		let unscaled = if self.is_negative() != other.is_negative() {
			magnitude.wrapping_neg()
		} else {
			magnitude
		};
		Self::from_parts(unscaled, scale)
	}

	pub fn checked_rem(&self, other: &Self) -> Option<Self> {
		if other.is_zero() {
			return None;
		}
		let scale = self.scale.max(other.scale);
		let dividend = self.unscaled.wrapping_abs();
		let magnitude = match unscaled::upscale(other.unscaled.wrapping_abs(), scale - other.scale) {
			Some(divisor) => {
				unscaled::shifted_remainder(dividend.wrapping_rem(divisor), divisor, scale - self.scale)
			}
			None => unscaled::upscale(dividend, scale - self.scale)?,
		};
		let unscaled = if self.is_negative() {
			magnitude.wrapping_neg()
		} else {
			magnitude
		};
		Self::from_parts(unscaled, scale)
	}

	pub fn saturating_add(&self, other: &Self) -> Self {
		self.checked_add(other)
			.unwrap_or_else(|| Self::saturated(*self < other.negate(), self.scale.max(other.scale)))
	}

	pub fn saturating_sub(&self, other: &Self) -> Self {
		self.checked_sub(other).unwrap_or_else(|| Self::saturated(*self < *other, self.scale.max(other.scale)))
	}

	pub fn saturating_mul(&self, other: &Self) -> Self {
		if let Some(product) = self.checked_mul(other) {
			return product;
		}
		let scale = u16::from(self.scale) + u16::from(other.scale);
		if scale > u16::from(MAX_DIGITS)
			&& let Some(product) = self.unscaled.checked_mul(other.unscaled)
		{
			let drop = u8::try_from(scale - u16::from(MAX_DIGITS)).unwrap_or(u8::MAX);
			return Self {
				unscaled: unscaled::round_half_up(product, drop),
				scale: MAX_DIGITS,
			};
		}
		Self::saturated(self.is_negative() != other.is_negative(), scale.min(u16::from(MAX_DIGITS)) as u8)
	}

	pub fn saturating_div(&self, other: &Self) -> Self {
		let scale = self.scale.max(other.scale).max(6);
		if other.is_zero() {
			return Self {
				unscaled: i256::ZERO,
				scale,
			};
		}
		self.checked_div(other)
			.unwrap_or_else(|| Self::saturated(self.is_negative() != other.is_negative(), scale))
	}

	pub fn saturating_rem(&self, other: &Self) -> Self {
		if other.is_zero() {
			return Self::zero();
		}
		self.checked_rem(other)
			.unwrap_or_else(|| Self::saturated(self.is_negative(), self.scale.max(other.scale)))
	}

	fn integer(unscaled: i256) -> Self {
		Self {
			unscaled,
			scale: 0,
		}
	}

	fn from_float_text(text: &str) -> Option<Self> {
		let (unscaled, scale) = Parsed::parse(text).ok()?.to_rounded_decimal().ok()?;
		Self::from_parts(unscaled, scale)
	}

	fn exponent_text(&self) -> String {
		format!("{}e-{}", self.unscaled, self.scale)
	}

	fn normalized(&self) -> (i256, u8) {
		let ten = i256::from_i128(10);
		let mut unscaled = self.unscaled;
		let mut scale = self.scale;
		while scale > 0 && unscaled.wrapping_rem(ten) == i256::ZERO {
			unscaled = unscaled.wrapping_div(ten);
			scale -= 1;
		}
		(unscaled, scale)
	}
}

impl PartialEq for Decimal {
	fn eq(&self, other: &Self) -> bool {
		self.normalized() == other.normalized()
	}
}

impl Eq for Decimal {}

impl PartialOrd for Decimal {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Decimal {
	fn cmp(&self, other: &Self) -> Ordering {
		let scale = self.scale.max(other.scale);
		let left = unscaled::upscale(self.unscaled, scale - self.scale);
		let right = unscaled::upscale(other.unscaled, scale - other.scale);
		match (left, right) {
			(Some(left), Some(right)) => left.cmp(&right),
			(None, _) if self.is_negative() => Ordering::Less,
			(None, _) => Ordering::Greater,
			(_, None) if other.is_negative() => Ordering::Greater,
			(_, None) => Ordering::Less,
		}
	}
}

impl hash::Hash for Decimal {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		self.normalized().hash(state);
	}
}

impl Display for Decimal {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let magnitude = self.unscaled.wrapping_abs().to_string();
		let sign = if self.is_negative() {
			"-"
		} else {
			""
		};
		let scale = self.scale as usize;
		if scale == 0 {
			return write!(f, "{sign}{magnitude}");
		}
		let padded = if magnitude.len() <= scale {
			format!("{}{}", "0".repeat(scale + 1 - magnitude.len()), magnitude)
		} else {
			magnitude
		};
		let (integer, fraction) = padded.split_at(padded.len() - scale);
		write!(f, "{sign}{integer}.{fraction}")
	}
}

impl FromStr for Decimal {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::parse(s).map_err(|error| parse_error(error, Fragment::None))
	}
}

pub(crate) fn parse_error(error: ParseError, fragment: Fragment) -> Error {
	match error {
		ParseError::Invalid => TypeError::InvalidNumberFormat {
			target: ValueType::DECIMAL,
			fragment,
		}
		.into(),
		ParseError::OutOfRange => TypeError::NumberOutOfRange {
			target: ValueType::DECIMAL,
			fragment,
			descriptor: None,
		}
		.into(),
	}
}

impl From<i8> for Decimal {
	fn from(value: i8) -> Self {
		Self::from_i128(value as i128)
	}
}

impl From<i16> for Decimal {
	fn from(value: i16) -> Self {
		Self::from_i128(value as i128)
	}
}

impl From<i32> for Decimal {
	fn from(value: i32) -> Self {
		Self::from_i128(value as i128)
	}
}

impl From<i64> for Decimal {
	fn from(value: i64) -> Self {
		Self::from_i128(value as i128)
	}
}

impl From<i128> for Decimal {
	fn from(value: i128) -> Self {
		Self::from_i128(value)
	}
}

impl From<u8> for Decimal {
	fn from(value: u8) -> Self {
		Self::from_i128(value as i128)
	}
}

impl From<u16> for Decimal {
	fn from(value: u16) -> Self {
		Self::from_i128(value as i128)
	}
}

impl From<u32> for Decimal {
	fn from(value: u32) -> Self {
		Self::from_i128(value as i128)
	}
}

impl From<u64> for Decimal {
	fn from(value: u64) -> Self {
		Self::from_i128(value as i128)
	}
}

impl From<u128> for Decimal {
	fn from(value: u128) -> Self {
		Self::from_u128(value)
	}
}

impl From<f32> for Decimal {
	fn from(value: f32) -> Self {
		if !value.is_finite() {
			return Self::zero();
		}
		Self::from_f32(value).unwrap_or_else(|| Self::saturated(value < 0.0, 0))
	}
}

impl From<f64> for Decimal {
	fn from(value: f64) -> Self {
		if !value.is_finite() {
			return Self::zero();
		}
		Self::from_f64(value).unwrap_or_else(|| Self::saturated(value < 0.0, 0))
	}
}

impl From<Int> for Decimal {
	fn from(value: Int) -> Self {
		Self::integer(value.to_i256())
	}
}

impl From<Uint> for Decimal {
	fn from(value: Uint) -> Self {
		Self::integer(value.to_i256())
	}
}

impl Serialize for Decimal {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.collect_str(self)
	}
}

struct DecimalVisitor;

impl<'de> Visitor<'de> for DecimalVisitor {
	type Value = Decimal;

	fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
		formatter.write_str("a decimal number as a string")
	}

	fn visit_str<E>(self, value: &str) -> Result<Decimal, E>
	where
		E: de::Error,
	{
		Decimal::parse(value).map_err(|error| E::custom(format!("invalid decimal {value:?}: {error:?}")))
	}
}

impl<'de> Deserialize<'de> for Decimal {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_str(DecimalVisitor)
	}
}

#[cfg(test)]
pub mod tests {
	use std::{
		collections::HashSet,
		hash::{DefaultHasher, Hash, Hasher},
	};

	use postcard::{from_bytes, to_stdvec};
	use serde_json::{from_str, to_string};

	use super::*;

	fn dec(text: &str) -> Decimal {
		Decimal::from_str(text).unwrap()
	}

	fn hash_of(decimal: &Decimal) -> u64 {
		let mut hasher = DefaultHasher::new();
		decimal.hash(&mut hasher);
		hasher.finish()
	}

	#[test]
	fn test_new_decimal_valid() {
		let decimal = Decimal::from_parts(i256::from_i128(12345), 2).unwrap();
		assert_eq!(decimal.to_string(), "123.45");
	}

	#[test]
	fn test_from_str() {
		let decimal = Decimal::from_str("123.45").unwrap();
		assert_eq!(decimal.to_string(), "123.45");
	}

	#[test]
	fn test_comparison() {
		let d1 = Decimal::from_str("123.45").unwrap();
		let d2 = Decimal::from_str("123.46").unwrap();
		let d3 = Decimal::from_str("123.45").unwrap();

		assert!(d1 < d2);
		assert_eq!(d1, d3);
	}

	#[test]
	fn test_display() {
		let decimal = Decimal::from_str("123.45").unwrap();
		assert_eq!(format!("{}", decimal), "123.45");
	}

	#[test]
	fn test_serde_json() {
		let decimal = Decimal::from_str("123.456789").unwrap();
		let json = to_string(&decimal).unwrap();
		assert_eq!(json, "\"123.456789\"");

		let deserialized: Decimal = from_str(&json).unwrap();
		assert_eq!(deserialized, decimal);
	}

	#[test]
	fn test_serde_json_negative() {
		let decimal = Decimal::from_str("-987.654321").unwrap();
		let json = to_string(&decimal).unwrap();
		assert_eq!(json, "\"-987.654321\"");

		let deserialized: Decimal = from_str(&json).unwrap();
		assert_eq!(deserialized, decimal);
	}

	#[test]
	fn test_serde_json_zero() {
		let decimal = Decimal::zero();
		let json = to_string(&decimal).unwrap();
		assert_eq!(json, "\"0\"");

		let deserialized: Decimal = from_str(&json).unwrap();
		assert_eq!(deserialized, decimal);
	}

	#[test]
	fn test_serde_json_high_precision() {
		let decimal = Decimal::from_str("123456789.123456789123456789").unwrap();
		let json = to_string(&decimal).unwrap();

		let deserialized: Decimal = from_str(&json).unwrap();
		assert_eq!(deserialized, decimal);
	}

	#[test]
	fn test_serde_postcard() {
		let decimal = Decimal::from_str("123.456789").unwrap();
		let encoded = to_stdvec(&decimal).unwrap();

		let decoded: Decimal = from_bytes(&encoded).unwrap();
		assert_eq!(decoded, decimal);
	}

	#[test]
	fn test_serde_postcard_negative() {
		let decimal = Decimal::from_str("-987.654321").unwrap();
		let encoded = to_stdvec(&decimal).unwrap();

		let decoded: Decimal = from_bytes(&encoded).unwrap();
		assert_eq!(decoded, decimal);
	}

	#[test]
	fn test_serde_postcard_zero() {
		let decimal = Decimal::zero();
		let encoded = to_stdvec(&decimal).unwrap();

		let decoded: Decimal = from_bytes(&encoded).unwrap();
		assert_eq!(decoded, decimal);
	}

	#[test]
	fn test_serde_postcard_high_precision() {
		let decimal = Decimal::from_str("123456789.123456789123456789").unwrap();
		let encoded = to_stdvec(&decimal).unwrap();

		let decoded: Decimal = from_bytes(&encoded).unwrap();
		assert_eq!(decoded, decimal);
	}

	#[test]
	fn test_serde_postcard_large_number() {
		let decimal = Decimal::from_str("999999999999999999999999999999.999999999999999999999999").unwrap();
		let encoded = to_stdvec(&decimal).unwrap();

		let decoded: Decimal = from_bytes(&encoded).unwrap();
		assert_eq!(decoded, decimal);
	}

	#[test]
	fn serde_keeps_the_scale_not_only_the_value() {
		// Equality ignores scale, so only the parts show whether a round trip normalised 1.50 to 1.5.
		for text in ["1.50", "-0.120", "0.00", "7"] {
			let decimal = dec(text);
			let back: Decimal = from_bytes(&to_stdvec(&decimal).unwrap()).unwrap();
			assert_eq!((back.unscaled(), back.scale()), (decimal.unscaled(), decimal.scale()), "{text}");
			let back: Decimal = from_str(&to_string(&decimal).unwrap()).unwrap();
			assert_eq!(back.to_string(), text);
		}
	}

	#[test]
	fn display_prints_exactly_scale_fraction_digits() {
		// Trimming or padding here changes what every decimal column renders.
		assert_eq!(Decimal::from_parts(i256::ZERO, 2).unwrap().to_string(), "0.00");
		assert_eq!(Decimal::from_parts(i256::from_i128(5), 3).unwrap().to_string(), "0.005");
		assert_eq!(Decimal::from_parts(i256::from_i128(-5), 3).unwrap().to_string(), "-0.005");
		assert_eq!(Decimal::from_parts(i256::from_i128(-12345), 3).unwrap().to_string(), "-12.345");
		assert_eq!(Decimal::from_parts(i256::from_i128(1000), 0).unwrap().to_string(), "1000");
		assert_eq!(
			Decimal::from_parts(unscaled::MIN, 76).unwrap().to_string(),
			format!("-0.{}", "9".repeat(76))
		);
	}

	#[test]
	fn from_parts_rejects_seventy_seven_digits_and_scales_past_seventy_six() {
		// Letting either through builds a value no Arrow decimal type can hold.
		assert!(Decimal::from_parts(unscaled::MAX, 76).is_some());
		assert_eq!(Decimal::from_parts(unscaled::MAX.wrapping_add(i256::ONE), 0), None);
		assert_eq!(Decimal::from_parts(unscaled::MIN.wrapping_sub(i256::ONE), 0), None);
		assert_eq!(Decimal::from_parts(i256::ONE, 77), None);
	}

	#[test]
	fn equality_order_and_hash_ignore_the_scale() {
		// Hash must agree with Eq, otherwise hashed collections split 1.5 and 1.50.
		assert_eq!(dec("1.5"), dec("1.50"));
		assert_eq!(hash_of(&dec("1.5")), hash_of(&dec("1.500")));
		assert_eq!(dec("0"), dec("0.000"));
		assert_eq!(hash_of(&dec("0")), hash_of(&dec("0.000")));
		assert_eq!(dec("1.5").cmp(&dec("1.50")), Ordering::Equal);
		assert!(dec("-1.51") < dec("-1.5"));
		assert!(dec("0.1") < dec("0.10000000001"));
		assert_ne!(dec("1.5"), dec("15"));
		let set: HashSet<Decimal> = ["1.5", "1.50", "1.500"].iter().map(|s| dec(s)).collect();
		assert_eq!(set.len(), 1);
	}

	#[test]
	fn order_holds_when_rescaling_would_overflow() {
		// Rescaling a 76 digit integer to scale 76 overflows i256; the order must still be exact.
		let huge = Decimal::from_parts(unscaled::MAX, 0).unwrap();
		let tiny = Decimal::from_parts(i256::ONE, 76).unwrap();
		assert!(huge > tiny);
		assert!(huge.negate() < tiny);
		assert!(tiny < huge);
		assert!(tiny > huge.negate());
		assert_ne!(huge, tiny);
	}

	#[test]
	fn from_str_rejects_past_seventy_six_digits_as_out_of_range() {
		// A 77 digit value must be a range error, never a wrapped or truncated number.
		assert_eq!(Decimal::from_str(&"1".repeat(77)).unwrap_err().code, "NUMBER_002");
		assert_eq!(Decimal::from_str("1.2.3").unwrap_err().code, "NUMBER_001");
		assert!(Decimal::from_str(&"9".repeat(76)).is_ok());
	}

	#[test]
	fn checked_add_and_sub_use_the_larger_scale() {
		// A result at the smaller scale would drop digits from the more precise operand.
		let sum = dec("1.5").checked_add(&dec("2.25")).unwrap();
		assert_eq!((sum.to_string(), sum.scale()), ("3.75".to_string(), 2));
		assert_eq!(dec("1").checked_sub(&dec("0.001")).unwrap().to_string(), "0.999");
		let max = Decimal::from_parts(unscaled::MAX, 0).unwrap();
		assert_eq!(max.checked_add(&Decimal::one()), None);
		assert_eq!(max.negate().checked_sub(&Decimal::one()), None);
		assert_eq!(max.checked_sub(&Decimal::one()).unwrap().digits(), 76);
	}

	#[test]
	fn checked_mul_adds_the_scales() {
		// Any other result scale disagrees with the type rule multiplication derives.
		assert_eq!(dec("1.50").checked_mul(&dec("2.0")).unwrap().to_string(), "3.000");
		assert_eq!(dec("-1.5").checked_mul(&dec("1.5")).unwrap().to_string(), "-2.25");
		let wide = Decimal::from_parts(unscaled::pow10(40).unwrap(), 0).unwrap();
		assert_eq!(wide.checked_mul(&wide), None);
		let deep = Decimal::from_parts(i256::ONE, 40).unwrap();
		assert_eq!(deep.checked_mul(&deep), None);
	}

	#[test]
	fn checked_div_uses_at_least_six_places_and_rounds_half_up() {
		// The result scale is max(left, right, 6); truncating would give 0.666666 for 2/3.
		assert_eq!(dec("2").checked_div(&dec("3")).unwrap().to_string(), "0.666667");
		assert_eq!(dec("-2").checked_div(&dec("3")).unwrap().to_string(), "-0.666667");
		assert_eq!(dec("3").checked_div(&dec("2")).unwrap().to_string(), "1.500000");
		assert_eq!(dec("1.00000000").checked_div(&dec("4")).unwrap().to_string(), "0.25000000");
		assert_eq!(dec("10").checked_div(&dec("0.001")).unwrap().to_string(), "10000.000000");
		assert_eq!(dec("1").checked_div(&dec("0")), None);
		let huge = Decimal::from_parts(unscaled::pow10(70).unwrap(), 0).unwrap();
		assert_eq!(huge.checked_div(&dec("3")).unwrap().digits(), 76);
		assert_eq!(huge.checked_div(&dec("0.1")), None);
	}

	#[test]
	fn checked_rem_keeps_the_dividend_sign_and_the_larger_scale() {
		// A remainder with the divisor sign or a lost fraction breaks x = q * y + r.
		assert_eq!(dec("7.5").checked_rem(&dec("2")).unwrap().to_string(), "1.5");
		assert_eq!(dec("-7.5").checked_rem(&dec("2")).unwrap().to_string(), "-1.5");
		assert_eq!(dec("7").checked_rem(&dec("0.3")).unwrap().to_string(), "0.1");
		assert_eq!(dec("7").checked_rem(&dec("0")), None);
		let huge = Decimal::from_parts(unscaled::MAX, 0).unwrap();
		let tenths = Decimal::from_parts(i256::from_i128(3), 1).unwrap();
		assert_eq!(huge.checked_rem(&tenths).unwrap().to_string(), "0.0");
		let small = dec("0.5");
		assert_eq!(small.checked_rem(&huge).unwrap().to_string(), "0.5");
		assert_eq!(dec("-0.5").checked_rem(&huge).unwrap().to_string(), "-0.5");
	}

	#[test]
	fn round_to_scale_rounds_half_away_from_zero() {
		// A cast rounds half up on the magnitude, so -1.235 becomes -1.24, not -1.23.
		assert_eq!(dec("1.235").round_to_scale(2).unwrap().to_string(), "1.24");
		assert_eq!(dec("-1.235").round_to_scale(2).unwrap().to_string(), "-1.24");
		assert_eq!(dec("1.234").round_to_scale(2).unwrap().to_string(), "1.23");
		assert_eq!(dec("0.125").round_to_scale(2).unwrap().to_string(), "0.13");
		assert_eq!(dec("0.0000001").round_to_scale(2).unwrap().to_string(), "0.00");
		assert_eq!(dec("1.5").round_to_scale(3).unwrap().to_string(), "1.500");
		let max = Decimal::from_parts(unscaled::MAX, 1).unwrap();
		assert_eq!(max.round_to_scale(0), Decimal::from_parts(unscaled::pow10(75).unwrap(), 0), "a carry fits");
		let widest = Decimal::from_parts(unscaled::MAX, 0).unwrap();
		assert_eq!(widest.round_to_scale(1), None, "padding 76 nines with a zero needs a 77th digit");
	}

	#[test]
	fn rescale_refuses_to_drop_fraction_digits() {
		// A write never rounds, so 1.235 cannot become a scale 2 value.
		assert_eq!(dec("1.235").rescale(2), None);
		assert_eq!(dec("1.230").rescale(2).unwrap().to_string(), "1.23");
		assert_eq!(dec("1.2").rescale(2).unwrap().to_string(), "1.20");
		assert_eq!(Decimal::from_parts(unscaled::MAX, 0).unwrap().rescale(1), None);
		assert_eq!(dec("1.2").fits(10, 2).unwrap().to_string(), "1.20");
		assert_eq!(dec("123456789.99").fits(10, 2), None);
		assert_eq!(dec("12345678.99").fits(10, 2).unwrap().to_string(), "12345678.99");
		assert_eq!(dec("1.235").fits(10, 2), None);
	}

	#[test]
	fn float_conversions_go_through_the_shortest_text() {
		// The shortest float text keeps 0.1 as 0.1 instead of its binary expansion.
		assert_eq!(Decimal::from_f64(0.1).unwrap().to_string(), "0.1");
		assert_eq!(Decimal::from_f64(-2.5e-3).unwrap().to_string(), "-0.0025");
		assert_eq!(Decimal::from_f64(1e20).unwrap().to_string(), "100000000000000000000");
		assert_eq!(Decimal::from_f64(1e-300).unwrap(), Decimal::zero());
		assert_eq!(Decimal::from_f64(1e80), None);
		assert_eq!(Decimal::from_f64(f64::NAN), None);
		assert_eq!(Decimal::from_f32(1.5f32).unwrap().to_string(), "1.5");
		assert_eq!(Decimal::from(f64::INFINITY), Decimal::zero());
		assert_eq!(Decimal::from(-1e80f64), Decimal::saturated(true, 0));
		assert_eq!(dec("0.1").to_f64(), 0.1);
		assert_eq!(dec("-12.5").to_f32(), -12.5f32);
		assert_eq!(Decimal::from_parts(unscaled::MAX, 0).unwrap().to_f64(), 1e76);
	}

	#[test]
	fn trunc_drops_the_fraction_toward_zero() {
		// Flooring would turn -3.7 into -4 where every integer conversion truncates.
		assert_eq!(dec("3.7").trunc(), i256::from_i128(3));
		assert_eq!(dec("-3.7").trunc(), i256::from_i128(-3));
		assert_eq!(dec("0.999").trunc(), i256::ZERO);
	}

	#[test]
	fn saturating_ops_clamp_to_seventy_six_nines_with_the_exact_sign() {
		// Clamping with the wrong sign, or to a value past 76 digits, stores a number nobody computed.
		let max = Decimal::from_parts(unscaled::MAX, 0).unwrap();
		let min = max.negate();
		assert_eq!(max.saturating_add(&Decimal::one()), max);
		assert_eq!(min.saturating_add(&Decimal::one().negate()), min);
		assert_eq!(min.saturating_sub(&Decimal::one()), min);
		assert_eq!(max.saturating_sub(&Decimal::one().negate()), max);
		assert_eq!(max.saturating_add(&dec("0.5")), Decimal::saturated(false, 1));
		assert_eq!(max.saturating_mul(&dec("-2")), min);
		assert_eq!(min.saturating_mul(&dec("-2")), max);
		assert_eq!(dec("1").saturating_div(&dec("0")).to_string(), "0.000000");
		assert_eq!(max.saturating_div(&dec("0.5")), Decimal::saturated(false, 6));
		assert_eq!(dec("7").saturating_rem(&dec("0")), Decimal::zero());
		assert_eq!(dec("1.5").saturating_add(&dec("1")).to_string(), "2.5");
	}

	#[test]
	fn saturating_mul_rounds_a_scale_past_seventy_six_instead_of_clamping() {
		// A tiny product must round toward zero, not jump to the largest representable value.
		let deep = Decimal::from_parts(i256::from_i128(15), 40).unwrap();
		let product = deep.saturating_mul(&deep);
		assert_eq!(product.scale(), 76);
		assert_eq!(product, Decimal::zero());
		let half = Decimal::from_parts(i256::from_i128(5), 40).unwrap();
		let unit = Decimal::from_parts(i256::ONE, 37).unwrap();
		assert_eq!(half.saturating_mul(&unit), Decimal::from_parts(i256::ONE, 76).unwrap());
		assert_eq!(half.negate().saturating_mul(&unit), Decimal::from_parts(i256::MINUS_ONE, 76).unwrap());
	}
}
