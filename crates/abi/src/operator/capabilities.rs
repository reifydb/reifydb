// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperatorCapability {
	Insert = 1 << 0,
	Update = 1 << 1,
	Delete = 1 << 2,
}

impl OperatorCapability {
	pub const STANDARD: &'static [OperatorCapability] =
		&[OperatorCapability::Insert, OperatorCapability::Update, OperatorCapability::Delete];

	pub const ALL: &'static [OperatorCapability] =
		&[OperatorCapability::Insert, OperatorCapability::Update, OperatorCapability::Delete];

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
	fn every_capability_bit_is_distinct() {
		// A shared bit would make two capabilities indistinguishable in the descriptor
		// bitmask, silently gating the wrong method on the plugin side.
		for (i, a) in OperatorCapability::ALL.iter().enumerate() {
			for b in &OperatorCapability::ALL[i + 1..] {
				assert_ne!(a.bit(), b.bit(), "{a:?} collides with {b:?}");
			}
		}
	}

	#[test]
	fn every_declared_capability_is_reachable_through_all() {
		// from_bitmask filters over ALL, so a variant missing from ALL is dropped on every
		// descriptor round trip and the operator loses that capability with no error anywhere. The
		// match is exhaustive so a new variant fails to compile here rather than vanishing at runtime.
		for capability in [OperatorCapability::Insert, OperatorCapability::Update, OperatorCapability::Delete] {
			match capability {
				OperatorCapability::Insert
				| OperatorCapability::Update
				| OperatorCapability::Delete => {}
			}
			assert!(
				OperatorCapability::ALL.contains(&capability),
				"{capability:?} is missing from ALL, so from_bitmask silently drops it"
			);
			assert!(
				from_bitmask(to_bitmask(&[capability])).contains(&capability),
				"{capability:?} does not survive a bitmask round trip"
			);
		}
	}

	#[test]
	fn presets_survive_a_bitmask_round_trip() {
		let restored = from_bitmask(to_bitmask(OperatorCapability::STANDARD));
		assert!(restored.contains(&OperatorCapability::Insert));
		assert!(restored.contains(&OperatorCapability::Update));
		assert!(restored.contains(&OperatorCapability::Delete));
	}

	#[test]
	fn an_unknown_descriptor_bit_is_dropped_rather_than_misread() {
		// Guests built against an older ABI may still set retired bits (the removed Reclaim
		// capability was 1 << 4); from_bitmask must ignore them instead of aliasing them onto a
		// live capability.
		let stale = to_bitmask(OperatorCapability::STANDARD) | (1 << 4);
		let restored = from_bitmask(stale);
		assert_eq!(restored, OperatorCapability::STANDARD.to_vec());
	}
}
