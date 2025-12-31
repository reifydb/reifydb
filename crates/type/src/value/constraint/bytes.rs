// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

/// Maximum bytes constraint for types like UTF8, BLOB, INT, UINT
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct MaxBytes(u32);

impl MaxBytes {
	/// Create a new MaxBytes value
	pub fn new(bytes: u32) -> Self {
		Self(bytes)
	}

	/// Get the max bytes value
	pub fn value(self) -> u32 {
		self.0
	}

	/// Maximum value (u32::MAX)
	pub const MAX: Self = Self(u32::MAX);

	/// Minimum value (0)
	pub const MIN: Self = Self(0);
}

impl Display for MaxBytes {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
