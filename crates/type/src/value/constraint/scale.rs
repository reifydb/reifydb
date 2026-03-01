// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use super::precision::Precision;
use crate::{
	error::{Error, TypeError},
	fragment::Fragment,
};

/// Scale for a decimal type (decimal places)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Scale(u8);

impl Scale {
	/// Create a new Scale value
	pub fn new(scale: u8) -> Self {
		Self(scale)
	}

	/// Create a new Scale value with validation against precision
	pub fn try_new_with_precision(scale: u8, precision: Precision) -> Result<Self, Error> {
		if scale > precision.value() {
			return Err(TypeError::DecimalScaleExceedsPrecision {
				scale,
				precision: precision.value(),
				fragment: Fragment::None,
			}
			.into());
		}
		Ok(Self(scale))
	}

	/// Get the scale value
	pub fn value(self) -> u8 {
		self.0
	}

	/// Maximum scale (255 - maximum u8 value)
	pub const MAX: Self = Self(255);

	/// Minimum scale (0)
	pub const MIN: Self = Self(0);
}

impl Display for Scale {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		self.0.fmt(f)
	}
}

impl From<Scale> for u8 {
	fn from(scale: Scale) -> Self {
		scale.0
	}
}

impl From<u8> for Scale {
	fn from(value: u8) -> Self {
		Self::new(value)
	}
}
