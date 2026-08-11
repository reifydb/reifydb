// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use crate::interface::catalog::{flow::FlowId, object::ObjectId};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperatorCapability {
	Insert = 1 << 0,
	Update = 1 << 1,
	Delete = 1 << 2,
	Expire = 1 << 5,
}

impl OperatorCapability {
	pub const STANDARD: &'static [OperatorCapability] =
		&[OperatorCapability::Insert, OperatorCapability::Update, OperatorCapability::Delete];

	pub const ALL: &'static [OperatorCapability] = &[
		OperatorCapability::Insert,
		OperatorCapability::Update,
		OperatorCapability::Delete,
		OperatorCapability::Expire,
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

#[derive(Debug, Clone)]
pub struct FlowWatermarkRow {
	pub flow_id: FlowId,

	pub object_id: ObjectId,

	pub lag: u64,

	pub outstanding: u64,
}

#[derive(Clone)]
pub struct FlowWatermarkSampler {
	fetch: Arc<dyn Fn() -> Vec<FlowWatermarkRow> + Send + Sync>,
}

impl FlowWatermarkSampler {
	pub fn new<F>(fetch: F) -> Self
	where
		F: Fn() -> Vec<FlowWatermarkRow> + Send + Sync + 'static,
	{
		Self {
			fetch: Arc::new(fetch),
		}
	}

	pub fn all(&self) -> Vec<FlowWatermarkRow> {
		(self.fetch)()
	}
}

#[cfg(test)]
mod tests {
	use super::{OperatorCapability, from_bitmask, to_bitmask};

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
		for capability in [
			OperatorCapability::Insert,
			OperatorCapability::Update,
			OperatorCapability::Delete,
			OperatorCapability::Expire,
		] {
			match capability {
				OperatorCapability::Insert
				| OperatorCapability::Update
				| OperatorCapability::Delete
				| OperatorCapability::Expire => {}
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
	fn expire_is_reachable_through_all_but_never_through_standard() {
		// STANDARD must never carry Expire, or every in-tree operator silently opts into rows it has no arm
		// for.
		assert!(!OperatorCapability::STANDARD.contains(&OperatorCapability::Expire));
		assert!(OperatorCapability::ALL.contains(&OperatorCapability::Expire));
		assert!(from_bitmask(to_bitmask(&[OperatorCapability::Expire])).contains(&OperatorCapability::Expire));
	}

	#[test]
	fn expire_does_not_claim_the_retired_reclaim_bit() {
		// Expire must never take the retired Reclaim bit, or a stale guest still setting it reads as opted in.
		assert_ne!(OperatorCapability::Expire.bit(), 1 << 4);
		assert!(!from_bitmask(1 << 4).contains(&OperatorCapability::Expire));
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
