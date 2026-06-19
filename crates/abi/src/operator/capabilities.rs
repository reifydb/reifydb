// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperatorCapability {
	Insert = 1 << 0,
	Update = 1 << 1,
	Delete = 1 << 2,
	Drop = 1 << 4,
	Tick = 1 << 5,
}

impl OperatorCapability {
	pub const STANDARD: &'static [OperatorCapability] =
		&[OperatorCapability::Insert, OperatorCapability::Update, OperatorCapability::Delete];

	pub const STANDARD_WITH_TICK: &'static [OperatorCapability] = &[
		OperatorCapability::Insert,
		OperatorCapability::Update,
		OperatorCapability::Delete,
		OperatorCapability::Tick,
	];

	pub const ALL: &'static [OperatorCapability] = &[
		OperatorCapability::Insert,
		OperatorCapability::Update,
		OperatorCapability::Delete,
		OperatorCapability::Drop,
		OperatorCapability::Tick,
	];

	pub const fn bit(self) -> u32 {
		self as u32
	}
}

pub fn to_bitmask(caps: &[OperatorCapability]) -> u32 {
	let mut mask = 0;
	for cap in caps {
		mask |= cap.bit();
	}
	mask
}

pub fn from_bitmask(mask: u32) -> Vec<OperatorCapability> {
	OperatorCapability::ALL.iter().copied().filter(|cap| mask & cap.bit() != 0).collect()
}
