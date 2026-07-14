// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::error::{Error, TypeError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Dimension(u32);

impl Dimension {
	pub fn new(dims: u32) -> Self {
		assert!(dims > 0, "Dimension must be at least 1, got {}", dims);
		Self(dims)
	}

	pub fn try_new(dims: u32) -> Result<Self, Error> {
		if dims == 0 {
			return Err(TypeError::VectorDimensionInvalid {
				dims,
			}
			.into());
		}
		Ok(Self(dims))
	}

	pub fn value(self) -> u32 {
		self.0
	}

	pub const MAX: Self = Self(u32::MAX);

	pub const MIN: Self = Self(1);
}

impl Display for Dimension {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		self.0.fmt(f)
	}
}

impl From<Dimension> for u32 {
	fn from(dimension: Dimension) -> Self {
		dimension.0
	}
}

impl From<u32> for Dimension {
	fn from(value: u32) -> Self {
		Self::new(value)
	}
}

impl From<Dimension> for usize {
	fn from(dimension: Dimension) -> Self {
		dimension.0 as usize
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn try_new_rejects_zero() {
		assert!(Dimension::try_new(0).is_err());
	}

	#[test]
	fn try_new_accepts_one() {
		assert_eq!(Dimension::try_new(1).unwrap().value(), 1);
	}

	#[test]
	#[should_panic(expected = "Dimension must be at least 1")]
	fn new_panics_on_zero() {
		Dimension::new(0);
	}

	#[test]
	fn converts_to_usize() {
		let dims: usize = Dimension::new(768).into();
		assert_eq!(dims, 768);
	}

	#[test]
	fn displays_as_plain_number() {
		assert_eq!(Dimension::new(768).to_string(), "768");
	}
}
