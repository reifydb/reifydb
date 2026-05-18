// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::error::{Error, TypeError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Precision(u8);

impl Precision {
	pub fn new(precision: u8) -> Self {
		assert!(precision > 0, "Precision must be at least 1, got {}", precision);
		Self(precision)
	}

	pub fn try_new(precision: u8) -> Result<Self, Error> {
		if precision == 0 {
			return Err(TypeError::DecimalPrecisionInvalid {
				precision,
			}
			.into());
		}
		Ok(Self(precision))
	}

	pub fn value(self) -> u8 {
		self.0
	}

	pub const MAX: Self = Self(255);

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
