// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct MaxBytes(u32);

impl MaxBytes {
	pub fn new(bytes: u32) -> Self {
		Self(bytes)
	}

	pub fn value(self) -> u32 {
		self.0
	}

	pub const MAX: Self = Self(u32::MAX);

	pub const MIN: Self = Self(0);
}

impl Display for MaxBytes {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		self.0.fmt(f)
	}
}

impl From<MaxBytes> for u32 {
	fn from(max_bytes: MaxBytes) -> Self {
		max_bytes.0
	}
}

impl From<u32> for MaxBytes {
	fn from(value: u32) -> Self {
		Self::new(value)
	}
}

impl From<MaxBytes> for usize {
	fn from(max_bytes: MaxBytes) -> Self {
		max_bytes.0 as usize
	}
}

impl From<usize> for MaxBytes {
	fn from(value: usize) -> Self {
		Self::new(value as u32)
	}
}
