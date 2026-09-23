// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::error::{Error, TypeError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Precision(u8);

impl Precision {
	pub const fn new(precision: u8) -> Self {
		assert!(precision >= Self::MIN.0 && precision <= Self::MAX.0, "precision must be between 1 and 76");
		Self(precision)
	}

	pub fn try_new(precision: u8) -> Result<Self, Error> {
		if !(Self::MIN.0..=Self::MAX.0).contains(&precision) {
			return Err(TypeError::DecimalPrecisionInvalid {
				precision,
			}
			.into());
		}
		Ok(Self(precision))
	}

	pub const fn value(self) -> u8 {
		self.0
	}

	pub const MAX: Self = Self(76);

	pub const MIN: Self = Self(1);
}

impl Display for Precision {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		self.0.fmt(f)
	}
}

impl From<Precision> for u8 {
	fn from(precision: Precision) -> Self {
		precision.0
	}
}

impl From<u8> for Precision {
	fn from(value: u8) -> Self {
		Self::new(value)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn precision_is_bounded_by_one_and_seventy_six() {
		// Precision 77 has no Arrow decimal type and precision 0 holds no digit, so both must be refused.
		assert_eq!(Precision::MAX.value(), 76);
		assert!(Precision::try_new(76).is_ok());
		assert!(Precision::try_new(1).is_ok());
		assert_eq!(Precision::try_new(77).unwrap_err().code, "NUMBER_006");
		assert_eq!(Precision::try_new(0).unwrap_err().code, "NUMBER_006");
	}

	#[test]
	#[should_panic(expected = "precision must be between 1 and 76")]
	fn new_panics_past_seventy_six() {
		// A silently accepted precision 77 would build a column no array can store.
		Precision::new(77);
	}
}
