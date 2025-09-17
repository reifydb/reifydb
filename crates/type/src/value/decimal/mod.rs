// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{
	cmp::Ordering,
	fmt::{Display, Formatter},
	ops::Deref,
	str::FromStr,
};

use bigdecimal::{BigDecimal as BigDecimalInner, FromPrimitive};
use num_traits::Zero;
use serde::{Deserialize, Serialize};

use super::{int::Int, uint::Uint};
use crate::{Error, OwnedFragment, Type, error};

mod parse;

pub use parse::parse_decimal;

use crate::error::diagnostic::number::invalid_number_format;

#[repr(transparent)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Decimal(pub BigDecimalInner);

impl Decimal {
	pub fn zero() -> Self {
		Self(BigDecimalInner::zero())
	}
}

impl Deref for Decimal {
	type Target = BigDecimalInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Decimal {
	pub fn new(value: BigDecimalInner) -> Self {
		Self(value)
	}

	pub fn from_bigdecimal(value: BigDecimalInner) -> Self {
		Self(value)
	}

	pub fn with_scale(value: BigDecimalInner, scale: i64) -> Self {
		Self(value.with_scale(scale))
	}

	pub fn from_i64(value: i64) -> Self {
		Self(BigDecimalInner::from(value))
	}

	pub fn inner(&self) -> &BigDecimalInner {
		&self.0
	}

	pub fn to_bigdecimal(self) -> BigDecimalInner {
		self.0
	}

	pub fn negate(self) -> Self {
		Self(-self.0)
	}
}

impl PartialEq for Decimal {
	fn eq(&self, other: &Self) -> bool {
		self.0 == other.0
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
		self.0.cmp(&other.0)
	}
}

impl Display for Decimal {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		self.0.fmt(f)
	}
}

impl std::hash::Hash for Decimal {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.0.to_string().hash(state);
	}
}

impl FromStr for Decimal {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let big_decimal = BigDecimalInner::from_str(s)
			.map_err(|_| error!(invalid_number_format(OwnedFragment::None, Type::Decimal)))?;

		Ok(Self(big_decimal))
	}
}

impl From<i64> for Decimal {
	fn from(value: i64) -> Self {
		Self(BigDecimalInner::from(value))
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
		Self(BigDecimalInner::from(value))
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
		Self(BigDecimalInner::from(value))
	}
}

impl From<u128> for Decimal {
	fn from(value: u128) -> Self {
		Self(BigDecimalInner::from(value))
	}
}

impl From<f32> for Decimal {
	fn from(value: f32) -> Self {
		let inner = BigDecimalInner::from_f32(value).unwrap_or_else(|| BigDecimalInner::from(0));
		Self(inner)
	}
}

impl From<f64> for Decimal {
	fn from(value: f64) -> Self {
		let inner = BigDecimalInner::from_f64(value).unwrap_or_else(|| BigDecimalInner::from(0));
		Self(inner)
	}
}

impl From<BigDecimalInner> for Decimal {
	fn from(value: BigDecimalInner) -> Self {
		Self(value)
	}
}

impl From<Int> for Decimal {
	fn from(value: Int) -> Self {
		Self(BigDecimalInner::from_bigint(value.0, 0))
	}
}

impl From<Uint> for Decimal {
	fn from(value: Uint) -> Self {
		Self(BigDecimalInner::from_bigint(value.0, 0))
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
		let decimal = Decimal::new(bd);
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
}
