// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use super::precision::Precision;
use crate::{
	error::{Error, TypeError},
	fragment::Fragment,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Scale(u8);

impl Scale {
	pub fn new(scale: u8) -> Self {
		Self(scale)
	}

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

	pub fn value(self) -> u8 {
		self.0
	}

	pub const MAX: Self = Self(255);

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
