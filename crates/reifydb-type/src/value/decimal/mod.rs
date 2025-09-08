// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{
	cmp::{Ordering, max},
	fmt::{Display, Formatter},
	str::FromStr,
};

use bigdecimal::{BigDecimal as BigDecimalInner, FromPrimitive};
use num_traits::Zero;
use serde::{Deserialize, Serialize};

use crate::{
	Error, OwnedFragment, Type, VarInt, VarUint, error,
	error::diagnostic::number::decimal_scale_exceeds_precision,
	return_error,
};

mod parse;
mod precision;
mod scale;

pub use parse::parse_decimal;
pub use precision::Precision;
pub use scale::Scale;

use crate::error::diagnostic::number::invalid_number_format;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Decimal {
	inner: BigDecimalInner,
	precision: Precision,
	scale: Scale,
}

impl Decimal {
	pub fn zero() -> Self {
		Self {
			inner: BigDecimalInner::zero(),
			precision: Precision::new(1),
			scale: Scale::new(0),
		}
	}
}

impl Decimal {
	pub fn new(
		value: BigDecimalInner,
		precision: Precision,
		scale: Scale,
	) -> Result<Self, Error> {
		if scale.value() > precision.value() {
			return_error!(decimal_scale_exceeds_precision(
				OwnedFragment::None,
				scale.value(),
				precision.value()
			));
		}

		let decimal = Self {
			inner: value,
			precision,
			scale,
		};

		Ok(decimal)
	}

	pub fn new_from_u8(
		value: BigDecimalInner,
		precision: u8,
		scale: u8,
	) -> Result<Self, Error> {
		let precision = Precision::try_new(precision)?;
		let scale = Scale::try_new_with_precision(scale, precision)?;
		Self::new(value, precision, scale)
	}

	pub fn with_scale(
		value: BigDecimalInner,
		scale: u8,
	) -> Result<Self, Error> {
		let precision_u8 = Self::calculate_min_precision(&value, scale);
		let precision = Precision::new(precision_u8);
		let scale = Scale::new(scale);
		Self::new(value, precision, scale)
	}

	pub fn from_i64(
		value: i64,
		precision: u8,
		scale: u8,
	) -> Result<Self, Error> {
		Self::new_from_u8(
			BigDecimalInner::from(value),
			precision,
			scale,
		)
	}

	pub fn from_str_with_precision(
		s: &str,
		precision: u8,
		scale: u8,
	) -> Result<Self, Error> {
		let big_decimal =
			BigDecimalInner::from_str(s).map_err(|_| {
				error!(invalid_number_format(
					OwnedFragment::None,
					Type::Decimal
				))
			})?;

		Self::new_from_u8(big_decimal, precision, scale)
	}

	pub fn precision(&self) -> Precision {
		self.precision
	}

	pub fn scale(&self) -> Scale {
		self.scale
	}

	pub fn inner(&self) -> &BigDecimalInner {
		&self.inner
	}

	pub fn to_bigdecimal(self) -> BigDecimalInner {
		self.inner
	}

	pub fn negate(self) -> Self {
		Self {
			inner: -self.inner,
			precision: self.precision,
			scale: self.scale,
		}
	}

	fn calculate_min_precision(
		value: &BigDecimalInner,
		min_scale: u8,
	) -> u8 {
		let str_repr = value.to_string();
		let digits: Vec<char> = str_repr
			.chars()
			.filter(|c| c.is_ascii_digit())
			.collect();
		let digit_count = digits.len() as u8;

		max(digit_count, min_scale)
	}
}

impl PartialEq for Decimal {
	fn eq(&self, other: &Self) -> bool {
		self.inner == other.inner
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
		self.inner.cmp(&other.inner)
	}
}

impl Display for Decimal {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		self.inner.fmt(f)
	}
}

impl std::hash::Hash for Decimal {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.inner.to_string().hash(state);
	}
}

impl FromStr for Decimal {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let big_decimal =
			BigDecimalInner::from_str(s).map_err(|_| {
				error!(invalid_number_format(
					OwnedFragment::None,
					Type::Decimal
				))
			})?;

		let scale_u8 = Self::determine_scale_from_string(s);
		let precision_u8 =
			Self::calculate_min_precision(&big_decimal, scale_u8);

		let precision = Precision::new(precision_u8);
		let scale = Scale::new(scale_u8);

		Self::new(big_decimal, precision, scale)
	}
}

impl Decimal {
	fn determine_scale_from_string(s: &str) -> u8 {
		if let Some(dot_pos) = s.find('.') {
			let after_dot = &s[dot_pos + 1..];
			after_dot.len() as u8
		} else {
			0
		}
	}
}

impl From<i64> for Decimal {
	fn from(value: i64) -> Self {
		let inner = BigDecimalInner::from(value);
		let precision_u8 = Self::calculate_min_precision(&inner, 0);
		Self::new(inner, Precision::new(precision_u8), Scale::new(0))
			.unwrap()
	}
}

impl From<i8> for Decimal {
	fn from(value: i8) -> Self {
		Self::from(value as i64)
	}
}

impl From<i16> for Decimal {
	fn from(value: i16) -> Self {
		Self::from(value as i64)
	}
}

impl From<i32> for Decimal {
	fn from(value: i32) -> Self {
		Self::from(value as i64)
	}
}

impl From<i128> for Decimal {
	fn from(value: i128) -> Self {
		let inner = BigDecimalInner::from(value);
		let precision_u8 = Self::calculate_min_precision(&inner, 0);
		Self::new(inner, Precision::new(precision_u8), Scale::new(0))
			.unwrap()
	}
}

impl From<u8> for Decimal {
	fn from(value: u8) -> Self {
		Self::from(value as i64)
	}
}

impl From<u16> for Decimal {
	fn from(value: u16) -> Self {
		Self::from(value as i64)
	}
}

impl From<u32> for Decimal {
	fn from(value: u32) -> Self {
		Self::from(value as i64)
	}
}

impl From<u64> for Decimal {
	fn from(value: u64) -> Self {
		let inner = BigDecimalInner::from(value);
		let precision_u8 = Self::calculate_min_precision(&inner, 0);
		Self::new(inner, Precision::new(precision_u8), Scale::new(0))
			.unwrap()
	}
}

impl From<u128> for Decimal {
	fn from(value: u128) -> Self {
		let inner = BigDecimalInner::from(value);
		let precision_u8 = Self::calculate_min_precision(&inner, 0);
		Self::new(inner, Precision::new(precision_u8), Scale::new(0))
			.unwrap()
	}
}

impl From<f32> for Decimal {
	fn from(value: f32) -> Self {
		let inner = BigDecimalInner::from_f32(value)
			.unwrap_or_else(|| BigDecimalInner::from(0));
		let scale_u8 =
			Self::determine_scale_from_string(&inner.to_string());
		let precision_u8 =
			Self::calculate_min_precision(&inner, scale_u8);
		Self::new(
			inner,
			Precision::new(precision_u8),
			Scale::new(scale_u8),
		)
		.unwrap()
	}
}

impl From<f64> for Decimal {
	fn from(value: f64) -> Self {
		let inner = BigDecimalInner::from_f64(value)
			.unwrap_or_else(|| BigDecimalInner::from(0));
		let scale_u8 =
			Self::determine_scale_from_string(&inner.to_string());
		let precision_u8 =
			Self::calculate_min_precision(&inner, scale_u8);
		Self::new(
			inner,
			Precision::new(precision_u8),
			Scale::new(scale_u8),
		)
		.unwrap()
	}
}

impl From<BigDecimalInner> for Decimal {
	fn from(value: BigDecimalInner) -> Self {
		let scale_u8 = 0;
		let precision_u8 =
			Self::calculate_min_precision(&value, scale_u8);

		Self::new(
			value,
			Precision::new(precision_u8),
			Scale::new(scale_u8),
		)
		.unwrap()
	}
}

impl From<VarInt> for Decimal {
	fn from(value: VarInt) -> Self {
		let value = BigDecimalInner::from_bigint(value.0, 0);
		let precision = Self::calculate_min_precision(&value, 0);

		Self::new(value, Precision::new(precision), Scale::new(0))
			.unwrap()
	}
}

impl From<VarUint> for Decimal {
	fn from(value: VarUint) -> Self {
		let value = BigDecimalInner::from_bigint(value.0, 0);
		let precision = Self::calculate_min_precision(&value, 0);

		Self::new(value, Precision::new(precision), Scale::new(0))
			.unwrap()
	}
}

impl Default for Decimal {
	fn default() -> Self {
		Self::zero()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_new_decimal_valid() {
		let bd = BigDecimalInner::from_str("123.45").unwrap();
		let decimal = Decimal::new_from_u8(bd, 5, 2).unwrap();
		assert_eq!(decimal.precision().value(), 5);
		assert_eq!(decimal.scale().value(), 2);
	}

	#[test]
	fn test_new_decimal_invalid_precision() {
		let bd = BigDecimalInner::from_str("123.45").unwrap();
		assert!(Decimal::new_from_u8(bd.clone(), 0, 2).is_err());
		// No upper limit on precision anymore
		assert!(Decimal::new_from_u8(bd, 100, 2).is_ok());
	}

	#[test]
	fn test_new_decimal_invalid_scale() {
		let bd = BigDecimalInner::from_str("123.45").unwrap();
		assert!(Decimal::new_from_u8(bd, 5, 6).is_err());
	}

	#[test]
	fn test_from_str() {
		let decimal = Decimal::from_str("123.45").unwrap();
		assert_eq!(decimal.to_string(), "123.45");
		assert_eq!(decimal.scale().value(), 2);
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
}
