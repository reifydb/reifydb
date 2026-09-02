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
use crate::{
	key::{operator::state::GroupId, typed::Key},
	state::typed::SuffixBytes,
};
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

		pub const REGISTERED: [u64; 4] = {
			let mut bits = [0u64; 4];
			$({
				let id = <$keyspace as Keyspace>::ID.0;
				bits[(id >> 6) as usize] |= 1u64 << (id & 63);
			})*
			bits
		};

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

		#[cfg(test)]
		fn every_keyspace_carries_its_group() {
			$(carries_its_group::<$keyspace>();)*
		}

		#[cfg(test)]
		fn every_keyspace_declares_its_shape() {
			$(declares_its_shape::<$keyspace>();)*
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
fn declares_its_shape<K: Keyspace>() {
	// GROUP_SCOPED is written by hand on all forty three impls and nothing stops it drifting from the key
	// it describes; a keyspace is group-scoped exactly when its key is its suffix behind one leading group
	// column, so the layout is the only witness that can contradict the declaration
	let key_columns = <K::Key as KeyLayout>::COLUMNS;
	let suffix_columns = <K::Suffix as KeyLayout>::COLUMNS;
	let carries_group = key_columns.len() == suffix_columns.len() + 1 && key_columns[0].name == "group";
	assert_eq!(
		K::GROUP_SCOPED,
		carries_group,
		"{}: GROUP_SCOPED disagrees with the key layout it is meant to describe",
		K::NAME
	);
}

#[cfg(test)]
fn carries_its_group<K: Keyspace>() {
	// a keyspace whose typed layout drops the group answers every group's read with one shared row: the
	// sqlite primary key loses the column, so writes from different groups overwrite each other and reads
	// come back stamped GroupId::ROOT. The suffix round trip alone cannot see it, because a join that
	// hardcodes ROOT still returns the suffix it was handed. A keyspace is group-scoped exactly when its
	// key is its suffix behind a leading group column; one whose key adds nothing to its suffix carries
	// the group as payload at most, is ROOT-only by construction, and must collapse every group to ROOT.
	let group_scoped = K::GROUP_SCOPED;
	let mut suffix = <K::Suffix as Key>::low();
	for step in 0..4 {
		for group in [GroupId::ROOT, GroupId(1), GroupId(9), GroupId(u128::MAX)] {
			let (back, _) = K::split(&K::join(group, suffix.clone()));
			if group_scoped {
				assert_eq!(
					back, group,
					"{}: step {step}: a group must survive join then split",
					K::NAME
				);
			} else {
				assert_eq!(
					back,
					GroupId::ROOT,
					"{}: step {step}: a keyspace with no group column must collapse every group to \
					 ROOT, or its writers believe a group is kept that the key cannot hold",
					K::NAME
				);
			}
		}
		if group_scoped {
			let distinct = K::join(GroupId(1), suffix.clone());
			let other = K::join(GroupId(9), suffix.clone());
			assert_ne!(
				distinct.to_suffix_bytes(),
				other.to_suffix_bytes(),
				"{}: step {step}: two groups holding the same suffix must not encode to one primary key",
				K::NAME
			);
		}
		match suffix.successor() {
			Some(next) => suffix = next,
			None => break,
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashSet;

	use super::{
		KEYSPACES, every_keyspace_carries_its_group, every_keyspace_declares_its_shape,
		every_keyspace_round_trips,
	};
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
	fn a_group_survives_join_and_split_for_every_keyspace() {
		// the group is the only thing separating one operator group's rows from another's; a keyspace that
		// declares the column but hardcodes GroupId::ROOT in split collapses them all onto one primary key,
		// so a sweep of group A reaps rows belonging to B and the rows A really holds are never named. The
		// converse half holds ROOT-only keyspaces to ROOT, so a writer cannot pass a group the key drops
		every_keyspace_carries_its_group();
	}

	#[test]
	fn every_keyspace_declares_the_scope_its_key_layout_shows() {
		// the declaration is what the write guard and the sweep both trust; if it says group-scoped while the
		// key holds no group column, every group's row collapses onto one primary key and the guard stays quiet
		every_keyspace_declares_its_shape();
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
