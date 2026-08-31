// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod distinct;
pub mod expiry;
pub mod join;
pub mod ringbuffer;
pub mod root;
pub mod timer;
pub mod window;

#[cfg(test)]
use crate::key::{operator::state::GroupId, typed::Key};
use crate::{
	interface::store::CacheTiers,
	key::{
		operator::{
			keyspace::{
				distinct::{DistinctEntry, DistinctLayout},
				expiry::{Expiry, ReapQueue, TumblingExpiry},
				join::{
					JoinLeft, JoinPin, JoinPublished, JoinRight, JoinRowExpiry, JoinRowMapping,
					JoinSchema,
				},
				ringbuffer::{
					PartitionedRingbufferEntry, PartitionedRingbufferExpiry,
					PartitionedRingbufferMeta, PartitionedRingbufferTtlArm, RingbufferEntry,
					RingbufferExpiry, RingbufferForward, RingbufferMeta, RingbufferTtlArm,
				},
				root::{
					CustomNotCached, GateVisibility, GroupRowMapping, GuestRowMapping, NodeCounter,
					SealLedger, SourceWatermark,
				},
				timer::{TimerIndex, TimerWheel},
				window::{
					Accumulator, Buffer, Count, Emit, EngineMeta, GuestAccumulator, GuestBuffer,
					GuestRunning, RollingMeta, RowIndex, Running, Session, WindowMeta,
				},
			},
			state::KeyspaceId,
			traits::Keyspace,
		},
		typed::layout::{KeyColumn, KeyLayout},
	},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyspaceSpec {
	pub name: &'static str,
	pub id: KeyspaceId,
	pub cache: CacheTiers,
	pub columns: &'static [KeyColumn],
	pub suffix: &'static [KeyColumn],
}

pub const fn columns_width(columns: &[KeyColumn]) -> usize {
	let mut width = 0;
	let mut index = 0;
	while index < columns.len() {
		width += columns[index].ty.width();
		index += 1;
	}
	width
}

impl KeyspaceSpec {
	pub const fn suffix_width(&self) -> usize {
		columns_width(self.suffix)
	}
}

pub trait KeyspaceVisitor {
	type Output;

	fn visit<K: Keyspace>(self) -> Self::Output;
}

macro_rules! catalogue {
	($($keyspace:ty),* $(,)?) => {
		pub const KEYSPACES: &[KeyspaceSpec] = &[
			$(KeyspaceSpec {
				name: <$keyspace as Keyspace>::NAME,
				id: <$keyspace as Keyspace>::ID,
				cache: <$keyspace as Keyspace>::CACHE,
				columns: <<$keyspace as Keyspace>::Key as KeyLayout>::COLUMNS,
				suffix: <<$keyspace as Keyspace>::Suffix as KeyLayout>::COLUMNS,
			}),*
		];

		pub fn suffix_width_of(id: KeyspaceId) -> Option<usize> {
			$(if id == <$keyspace as Keyspace>::ID {
				return Some(columns_width(<<$keyspace as Keyspace>::Suffix as KeyLayout>::COLUMNS));
			})*
			None
		}

		pub fn dispatch<V: KeyspaceVisitor>(id: KeyspaceId, visitor: V) -> Option<V::Output> {
			$(if id == <$keyspace as Keyspace>::ID {
				return Some(visitor.visit::<$keyspace>());
			})*
			None
		}

		#[cfg(test)]
		fn every_keyspace_round_trips() {
			$(round_trips::<$keyspace>();)*
		}
	};
}

catalogue!(
	Accumulator,
	Buffer,
	Running,
	Count,
	Session,
	RollingMeta,
	EngineMeta,
	Emit,
	RowIndex,
	WindowMeta,
	GuestAccumulator,
	GuestBuffer,
	GuestRunning,
	JoinLeft,
	JoinRight,
	JoinPublished,
	JoinPin,
	JoinSchema,
	JoinRowExpiry,
	JoinRowMapping,
	RingbufferForward,
	RingbufferEntry,
	RingbufferExpiry,
	RingbufferTtlArm,
	RingbufferMeta,
	PartitionedRingbufferEntry,
	PartitionedRingbufferExpiry,
	PartitionedRingbufferTtlArm,
	PartitionedRingbufferMeta,
	TimerWheel,
	TimerIndex,
	Expiry,
	TumblingExpiry,
	ReapQueue,
	DistinctEntry,
	DistinctLayout,
	SourceWatermark,
	SealLedger,
	NodeCounter,
	GateVisibility,
	GroupRowMapping,
	GuestRowMapping,
	CustomNotCached,
);

#[cfg(test)]
fn round_trips<K: Keyspace>() {
	// low() is every column at the start of its own order, so a join that hardcoded a column to its
	// minimum would round trip against low() alone; stepping the suffix first is what makes the
	// probe able to fail
	let mut suffix = <K::Suffix as Key>::low();
	for step in 0..4 {
		let key = K::join(GroupId(9), suffix.clone());
		let (group, split) = K::split(&key);
		assert_eq!(split, suffix, "{}: step {step}: a suffix must survive join then split", K::NAME);
		assert_eq!(
			K::join(group, split.clone()),
			key,
			"{}: step {step}: split then join must return the same key",
			K::NAME
		);
		let (again_group, again_split) = K::split(&K::join(group, split.clone()));
		assert_eq!(
			(again_group, again_split),
			(group, split),
			"{}: step {step}: a second round trip must not drift, or the container's identity is \
			 lost on every rewrite",
			K::NAME
		);
		match suffix.successor() {
			Some(next) => suffix = next,
			None => break,
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashSet;

	use super::{KEYSPACES, every_keyspace_round_trips};
	use crate::{interface::store::CacheTiers, key::operator::state::KeyspaceId};

	fn catalogue() -> Vec<(&'static str, KeyspaceId, CacheTiers)> {
		KEYSPACES.iter().map(|spec| (spec.name, spec.id, spec.cache)).collect()
	}

	#[test]
	fn every_keyspace_names_and_tiers_itself_the_way_its_id_does() {
		// the impl writes NAME and CACHE down by hand and KeyspaceId answers them separately, so this is
		// the only place the two lists are forced to agree; a keyspace that quietly changed tiers on one
		// side would otherwise be cached by the store and uncached by the catalogue
		for (name, id, cache) in catalogue() {
			assert_eq!(name, id.name(), "{name} and its id disagree on the name");
			assert_eq!(cache, id.cache_tiers(), "{name} and its id disagree on the cache tiers");
		}
	}

	#[test]
	fn no_two_keyspaces_claim_the_same_id() {
		// two keyspaces on one id is exactly the bug R20, R22 and R25 exist to undo, and it is invisible
		// at runtime: the second one's rows simply decode as the first one's shape
		let mut seen = HashSet::new();
		for (name, id, _) in catalogue() {
			assert!(seen.insert(id), "{name} reuses an id another keyspace already claims");
		}
		assert_eq!(seen.len(), 43, "the catalogue is forty three keyspaces");
	}

	#[test]
	fn join_and_split_are_inverse_for_every_keyspace() {
		// R15: the range tier stores only the suffix and rebuilds the key from its partition identity, so
		// a lossy split silently drops a key column on every read back
		every_keyspace_round_trips();
	}

	#[test]
	fn the_catalogue_covers_every_id_the_substrate_declares() {
		// A declared id with no keyspace has no typed key, so its writers fall back to raw suffix bytes
		// nothing round-trips and the range tier rebuilds a key it cannot decode. There is no reflection
		// over associated constants, so counting the declarations in the source is the only way to notice
		// an id nobody gave a keyspace; 0xFE was the last one and S10 retired it.
		let source = include_str!("../state.rs");
		let body = source
			.split("impl KeyspaceId {")
			.nth(1)
			.expect("the KeyspaceId impl block is where the constants are declared");
		let declared = body
			.split("\n}\n")
			.next()
			.expect("the impl block is closed")
			.lines()
			.filter(|line| {
				let line = line.trim_start();
				line.starts_with("pub const") && line.contains("Self(")
			})
			.count();
		assert_eq!(
			catalogue().len(),
			declared,
			"the substrate declares {declared} keyspace ids and the catalogue types {}",
			catalogue().len()
		);
	}
}

