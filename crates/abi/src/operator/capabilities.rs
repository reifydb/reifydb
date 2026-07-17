// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperatorCapability {
	Insert = 1 << 0,
	Update = 1 << 1,
	Delete = 1 << 2,
	Sample = 1 << 3,
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

	pub const STANDARD_WITH_SAMPLE: &'static [OperatorCapability] = &[
		OperatorCapability::Insert,
		OperatorCapability::Update,
		OperatorCapability::Delete,
		OperatorCapability::Sample,
	];

	pub const STANDARD_WITH_TICK_SAMPLE: &'static [OperatorCapability] = &[
		OperatorCapability::Insert,
		OperatorCapability::Update,
		OperatorCapability::Delete,
		OperatorCapability::Sample,
		OperatorCapability::Tick,
	];

	pub const ALL: &'static [OperatorCapability] = &[
		OperatorCapability::Insert,
		OperatorCapability::Update,
		OperatorCapability::Delete,
		OperatorCapability::Sample,
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn sample_bit_is_distinct_from_every_other_capability() {
		// A shared bit would make Sample indistinguishable from another
		// capability in the descriptor bitmask, silently gating the wrong method.
		for cap in OperatorCapability::ALL {
			if *cap != OperatorCapability::Sample {
				assert_ne!(cap.bit(), OperatorCapability::Sample.bit(), "{cap:?} collides with Sample");
			}
		}
	}

	#[test]
	fn sample_survives_a_bitmask_round_trip() {
		// from_bitmask filters over ALL, so a capability missing from ALL would
		// be dropped on the way back - this pins Sample into that round trip.
		let caps = OperatorCapability::STANDARD_WITH_SAMPLE;
		let restored = from_bitmask(to_bitmask(caps));
		assert!(
			restored.contains(&OperatorCapability::Sample),
			"Sample must round-trip through the descriptor bitmask"
		);
		assert!(restored.contains(&OperatorCapability::Insert));
		assert!(!restored.contains(&OperatorCapability::Tick), "STANDARD_WITH_SAMPLE must not imply Tick");
	}

	#[test]
	fn tick_sample_preset_carries_both_bits() {
		let mask = to_bitmask(OperatorCapability::STANDARD_WITH_TICK_SAMPLE);
		assert_ne!(mask & OperatorCapability::Sample.bit(), 0);
		assert_ne!(mask & OperatorCapability::Tick.bit(), 0);
	}
}
