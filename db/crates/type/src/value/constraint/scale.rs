// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use super::precision::Precision;
use crate::{Error, OwnedFragment, error::diagnostic::number::decimal_scale_exceeds_precision, return_error};

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
			return_error!(decimal_scale_exceeds_precision(OwnedFragment::None, scale, precision.value()));
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
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
