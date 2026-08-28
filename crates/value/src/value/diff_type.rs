// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use serde::{Deserialize, Serialize};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiffType {
	Insert = 1,

	Update = 2,

	Remove = 3,
}

impl DiffType {
	#[inline]
	pub const fn as_u8(self) -> u8 {
		self as u8
	}

	#[inline]
	pub const fn from_u8(raw: u8) -> Option<Self> {
		match raw {
			1 => Some(DiffType::Insert),
			2 => Some(DiffType::Update),
			3 => Some(DiffType::Remove),
			_ => None,
		}
	}
}
