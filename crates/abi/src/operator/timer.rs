// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TimerKind {
	Seal = 0,
	Grace = 1,
	RowTtl = 2,
	Maintenance = 3,
}

impl TimerKind {
	pub fn is_unique(&self) -> bool {
		matches!(self, Self::Maintenance)
	}

	pub fn from_u8(value: u8) -> Option<Self> {
		match value {
			0 => Some(Self::Seal),
			1 => Some(Self::Grace),
			2 => Some(Self::RowTtl),
			3 => Some(Self::Maintenance),
			_ => None,
		}
	}
}
