// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encode_u8,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::KeyKind;
use crate::interface::catalog::flow::OperatorId;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(pub u64);

impl GroupId {
	pub const NODE_SCOPE: Self = Self(0);

	pub const FIRST: Self = Self(1);

	pub fn is_node_scope(&self) -> bool {
		*self == Self::NODE_SCOPE
	}
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupSet(Vec<GroupId>);

impl GroupSet {
	pub fn new(groups: impl IntoIterator<Item = GroupId>) -> Self {
		let mut groups: Vec<GroupId> = groups.into_iter().filter(|g| !g.is_node_scope()).collect();
		groups.sort_unstable();
		groups.dedup();
		Self(groups)
	}

	pub fn contains(&self, group: GroupId) -> bool {
		self.0.binary_search(&group).is_ok()
	}

	pub fn as_slice(&self) -> &[GroupId] {
		&self.0
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn as_raw_parts(&self) -> (*const u64, usize) {
		(self.0.as_ptr() as *const u64, self.0.len())
	}
}

pub fn group_data_of_inner(inner: &[u8]) -> Option<GroupId> {
	let mut de = KeyDeserializer::from_bytes(inner);
	let group = GroupId(de.read_u64().ok()?);
	let keyspace = Keyspace(de.read_u8().ok()?);
	if !keyspace.is_data() {
		return None;
	}
	inner.starts_with(&group_inner_prefix(group)).then_some(group)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Keyspace(pub u8);

impl Keyspace {
	pub const HIGHEST_DATA: u8 = 0x7F;

	pub const ROW_NUMBER_MAPPING: Self = Self(0xFE);

	pub const GROUP_DICTIONARY: Self = Self(0xFD);

	pub const NODE_COUNTER: Self = Self(0xFC);

	pub const GROUP_RECORD: Self = Self(0xFB);

	pub const ACTIVITY_INDEX: Self = Self(0xFA);

	pub const IDENTITY_INDEX: Self = Self(0xF9);

	pub const NODE_WATERMARK: Self = Self(0xF8);

	pub const SOURCE_WATERMARK: Self = Self(0xF7);

	pub const TIMER_WHEEL: Self = Self(0xF6);

	pub const SIDE_ACTIVITY_INDEX: Self = Self(0xF5);

	pub const SIDE_ACTIVITY_RECORD: Self = Self(0xF4);

	pub const ACCUMULATOR: Self = Self(0x10);

	pub const BUFFER: Self = Self(0x11);

	pub const RUNNING: Self = Self(0x12);

	pub const EMIT: Self = Self(0x13);

	pub const EXPIRY: Self = Self(0x14);

	pub const COUNT: Self = Self(0x16);

	pub const ROW_INDEX: Self = Self(0x17);

	pub const SESSION: Self = Self(0x18);

	pub const ROLLING_META: Self = Self(0x19);

	pub const ENGINE_META: Self = Self(0x1A);

	pub const DISTINCT_ENTRY: Self = Self(0x1B);

	pub const WINDOW_META: Self = Self(0x1C);

	pub const JOIN_LEFT: Self = Self(0x1D);

	pub const JOIN_RIGHT: Self = Self(0x1E);

	pub const JOIN_SCHEMA: Self = Self(0x1F);

	pub const RINGBUFFER_FORWARD: Self = Self(0x20);

	pub const RINGBUFFER_ENTRY: Self = Self(0x21);

	pub const GATE_VISIBILITY: Self = Self(0x22);

	pub const DISTINCT_LAYOUT: Self = Self(0x23);

	pub const RINGBUFFER_EXPIRY: Self = Self(0x24);

	pub const RINGBUFFER_TTL_ARM: Self = Self(0x25);

	pub const SEAL_LEDGER: Self = Self(0x26);

	pub const JOIN_PUBLISHED: Self = Self(0x27);

	pub const JOIN_PIN: Self = Self(0x28);

	pub const FIRST_CUSTOM: Self = Self(0x40);

	pub fn is_data(&self) -> bool {
		self.0 <= Self::HIGHEST_DATA
	}

	pub fn is_identity(&self) -> bool {
		!self.is_data()
	}

	pub fn is_known(&self) -> bool {
		self.is_data()
			|| matches!(
				*self,
				Self::ROW_NUMBER_MAPPING
					| Self::GROUP_DICTIONARY | Self::NODE_COUNTER
					| Self::GROUP_RECORD | Self::ACTIVITY_INDEX
					| Self::IDENTITY_INDEX | Self::NODE_WATERMARK
					| Self::SOURCE_WATERMARK | Self::TIMER_WHEEL
					| Self::SIDE_ACTIVITY_INDEX | Self::SIDE_ACTIVITY_RECORD
			)
	}
}

pub fn is_framed_inner(inner: &[u8]) -> bool {
	inner.is_empty() || OperatorGroupStateKey::decode_inner(inner).is_some_and(|(_, keyspace, _)| keyspace.is_known())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorGroupStateKey {
	pub node: OperatorId,
	pub group: GroupId,
	pub keyspace: Keyspace,
	pub suffix: Vec<u8>,
}

impl OperatorGroupStateKey {
	pub fn new(node: OperatorId, group: GroupId, keyspace: Keyspace, suffix: impl Into<Vec<u8>>) -> Self {
		Self {
			node,
			group,
			keyspace,
			suffix: suffix.into(),
		}
	}

	pub fn node_scoped(node: OperatorId, keyspace: Keyspace, suffix: impl Into<Vec<u8>>) -> Self {
		Self::new(node, GroupId::NODE_SCOPE, keyspace, suffix)
	}

	pub fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(20 + self.suffix.len());
		serializer
			.extend_u8(KeyKind::OperatorState as u8)
			.extend_u64(self.node.0)
			.extend_u64(self.group.0)
			.extend_u8(self.keyspace.0)
			.extend_raw(&self.suffix);
		serializer.to_encoded_key()
	}

	pub fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != KeyKind::OperatorState {
			return None;
		}

		let node = de.read_u64().ok()?;
		let group = de.read_u64().ok()?;
		let keyspace = de.read_u8().ok()?;
		let suffix = de.read_raw(de.remaining()).ok()?.to_vec();

		Some(Self {
			node: OperatorId(node),
			group: GroupId(group),
			keyspace: Keyspace(keyspace),
			suffix,
		})
	}

	pub fn encoded(node: OperatorId, group: GroupId, keyspace: Keyspace, suffix: impl AsRef<[u8]>) -> EncodedKey {
		let suffix = suffix.as_ref();
		let mut serializer = KeySerializer::with_capacity(20 + suffix.len());
		serializer
			.extend_u8(KeyKind::OperatorState as u8)
			.extend_u64(node.0)
			.extend_u64(group.0)
			.extend_u8(keyspace.0)
			.extend_raw(suffix);
		serializer.to_encoded_key()
	}

	pub fn inner(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(12 + self.suffix.len());
		serializer.extend_u64(self.group.0).extend_u8(self.keyspace.0).extend_raw(&self.suffix);
		serializer.to_encoded_key()
	}

	pub fn inner_encoded(group: GroupId, keyspace: Keyspace, suffix: impl AsRef<[u8]>) -> GroupStateKey {
		let suffix = suffix.as_ref();
		let mut serializer = KeySerializer::with_capacity(12 + suffix.len());
		serializer.extend_u64(group.0).extend_u8(keyspace.0).extend_raw(suffix);
		GroupStateKey(serializer.to_encoded_key())
	}

	pub fn decode_inner(inner: &[u8]) -> Option<(GroupId, Keyspace, Vec<u8>)> {
		let mut de = KeyDeserializer::from_bytes(inner);
		let group = de.read_u64().ok()?;
		let keyspace = de.read_u8().ok()?;
		let suffix = de.read_raw(de.remaining()).ok()?.to_vec();
		Some((GroupId(group), Keyspace(keyspace), suffix))
	}
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupStateKey(EncodedKey);

impl GroupStateKey {
	pub fn new(group: GroupId, keyspace: Keyspace, suffix: impl AsRef<[u8]>) -> Self {
		OperatorGroupStateKey::inner_encoded(group, keyspace, suffix)
	}

	pub fn node_scoped(keyspace: Keyspace, suffix: impl AsRef<[u8]>) -> Self {
		Self::new(GroupId::NODE_SCOPE, keyspace, suffix)
	}

	pub fn from_framed(key: EncodedKey) -> Option<Self> {
		is_framed_inner(key.as_slice()).then_some(Self(key))
	}

	pub fn as_encoded(&self) -> &EncodedKey {
		&self.0
	}

	pub fn into_encoded(self) -> EncodedKey {
		self.0
	}

	pub fn as_slice(&self) -> &[u8] {
		self.0.as_slice()
	}

	pub fn as_bytes(&self) -> &[u8] {
		self.0.as_bytes()
	}

	pub fn group(&self) -> Option<GroupId> {
		OperatorGroupStateKey::decode_inner(self.0.as_slice()).map(|(group, _, _)| group)
	}
}

impl AsRef<[u8]> for GroupStateKey {
	fn as_ref(&self) -> &[u8] {
		self.0.as_slice()
	}
}

impl AsRef<EncodedKey> for GroupStateKey {
	fn as_ref(&self) -> &EncodedKey {
		&self.0
	}
}

pub trait IntoGroupStateKey {
	fn into_group_state_key(self) -> GroupStateKey;
}

impl IntoGroupStateKey for GroupStateKey {
	fn into_group_state_key(self) -> GroupStateKey {
		self
	}
}

fn group_inner_prefix(group: GroupId) -> Vec<u8> {
	let mut serializer = KeySerializer::with_capacity(12);
	serializer.extend_u64(group.0);
	serializer.finish().as_ref().to_vec()
}

fn keyspace_inner_prefix(group: GroupId, keyspace: Keyspace) -> Vec<u8> {
	let mut prefix = group_inner_prefix(group);
	prefix.push(encode_u8(keyspace.0));
	prefix
}

pub fn group_inner_range(group: GroupId) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&group_inner_prefix(group))
}

pub fn keyspace_inner_range(group: GroupId, keyspace: Keyspace) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&keyspace_inner_prefix(group, keyspace))
}

pub fn group_data_inner_range(group: GroupId) -> EncodedKeyRange {
	let prefix = group_inner_prefix(group);
	let mut start = prefix.clone();
	start.push(encode_u8(Keyspace::HIGHEST_DATA));
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(start)), EncodedKeyRange::prefix(&prefix).end)
}

pub fn group_identity_inner_range(group: GroupId) -> EncodedKeyRange {
	let prefix = group_inner_prefix(group);
	let mut end = prefix.clone();
	end.push(encode_u8(Keyspace::HIGHEST_DATA));
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(prefix)), Bound::Excluded(EncodedKey::new(end)))
}

fn node_prefix(node: OperatorId) -> Vec<u8> {
	let mut serializer = KeySerializer::with_capacity(12);
	serializer.extend_u8(KeyKind::OperatorState as u8).extend_u64(node.0);
	serializer.finish().as_ref().to_vec()
}

fn group_prefix(node: OperatorId, group: GroupId) -> Vec<u8> {
	let mut serializer = KeySerializer::with_capacity(20);
	serializer.extend_u8(KeyKind::OperatorState as u8).extend_u64(node.0).extend_u64(group.0);
	serializer.finish().as_ref().to_vec()
}

fn keyspace_prefix(node: OperatorId, group: GroupId, keyspace: Keyspace) -> Vec<u8> {
	let mut prefix = group_prefix(node, group);
	prefix.push(encode_u8(keyspace.0));
	prefix
}

pub fn node_range(node: OperatorId) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&node_prefix(node))
}

pub fn group_range(node: OperatorId, group: GroupId) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&group_prefix(node, group))
}

pub fn keyspace_range(node: OperatorId, group: GroupId, keyspace: Keyspace) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&keyspace_prefix(node, group, keyspace))
}

pub fn group_data_range(node: OperatorId, group: GroupId) -> EncodedKeyRange {
	let prefix = group_prefix(node, group);
	let mut start = prefix.clone();
	start.push(encode_u8(Keyspace::HIGHEST_DATA));
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(start)), EncodedKeyRange::prefix(&prefix).end)
}

pub fn group_identity_range(node: OperatorId, group: GroupId) -> EncodedKeyRange {
	let prefix = group_prefix(node, group);
	let mut end = prefix.clone();
	end.push(encode_u8(Keyspace::HIGHEST_DATA));
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(prefix)), Bound::Excluded(EncodedKey::new(end)))
}

#[cfg(test)]
mod tests {
	use std::{ops::Bound, slice};

	use super::{
		EncodedKeyRange, GroupId, GroupSet, KeySerializer, Keyspace, OperatorGroupStateKey, group_data_inner_range,
		group_data_of_inner, group_data_range, group_identity_inner_range, group_identity_range,
		group_inner_prefix, group_inner_range, group_range, is_framed_inner, keyspace_range, node_range,
	};
	use crate::{
		interface::{
			catalog::flow::OperatorId,
			store::{EntryKind, classify_key},
		},
		key::{EncodableKey, operator_state::OperatorStateKey},
	};

	const NODES: [u64; 4] = [1, 17, 300, 70_000];
	const GROUPS: [u64; 8] = [1, 2, 127, 128, 1000, 100_000, 1 << 30, u64::MAX];
	const DATA_KEYSPACES: [Keyspace; 4] =
		[Keyspace::ACCUMULATOR, Keyspace::BUFFER, Keyspace::RUNNING, Keyspace::FIRST_CUSTOM];
	const IDENTITY_KEYSPACES: [Keyspace; 2] = [Keyspace::GROUP_RECORD, Keyspace::ROW_NUMBER_MAPPING];

	#[derive(Clone, Copy, PartialEq, Debug)]
	enum Phase {
		Data,
		Identity,
	}

	/// Every keyspace the substrate declares, with the phase allowed to erase it. The phase is written
	/// down rather than read back from `is_data`, or a keyspace changing sides would pass unremarked.
	const CENSUS: [(&str, Keyspace, Phase); 36] = [
		("ROW_NUMBER_MAPPING", Keyspace::ROW_NUMBER_MAPPING, Phase::Identity),
		("GROUP_DICTIONARY", Keyspace::GROUP_DICTIONARY, Phase::Identity),
		("NODE_COUNTER", Keyspace::NODE_COUNTER, Phase::Identity),
		("GROUP_RECORD", Keyspace::GROUP_RECORD, Phase::Identity),
		("ACTIVITY_INDEX", Keyspace::ACTIVITY_INDEX, Phase::Identity),
		("IDENTITY_INDEX", Keyspace::IDENTITY_INDEX, Phase::Identity),
		("NODE_WATERMARK", Keyspace::NODE_WATERMARK, Phase::Identity),
		("SOURCE_WATERMARK", Keyspace::SOURCE_WATERMARK, Phase::Identity),
		("TIMER_WHEEL", Keyspace::TIMER_WHEEL, Phase::Identity),
		("SIDE_ACTIVITY_INDEX", Keyspace::SIDE_ACTIVITY_INDEX, Phase::Identity),
		("SIDE_ACTIVITY_RECORD", Keyspace::SIDE_ACTIVITY_RECORD, Phase::Identity),
		("ACCUMULATOR", Keyspace::ACCUMULATOR, Phase::Data),
		("BUFFER", Keyspace::BUFFER, Phase::Data),
		("RUNNING", Keyspace::RUNNING, Phase::Data),
		("EMIT", Keyspace::EMIT, Phase::Data),
		("EXPIRY", Keyspace::EXPIRY, Phase::Data),
		("COUNT", Keyspace::COUNT, Phase::Data),
		("ROW_INDEX", Keyspace::ROW_INDEX, Phase::Data),
		("SESSION", Keyspace::SESSION, Phase::Data),
		("ROLLING_META", Keyspace::ROLLING_META, Phase::Data),
		("ENGINE_META", Keyspace::ENGINE_META, Phase::Data),
		("DISTINCT_ENTRY", Keyspace::DISTINCT_ENTRY, Phase::Data),
		("WINDOW_META", Keyspace::WINDOW_META, Phase::Data),
		("JOIN_LEFT", Keyspace::JOIN_LEFT, Phase::Data),
		("JOIN_RIGHT", Keyspace::JOIN_RIGHT, Phase::Data),
		("JOIN_SCHEMA", Keyspace::JOIN_SCHEMA, Phase::Data),
		("RINGBUFFER_FORWARD", Keyspace::RINGBUFFER_FORWARD, Phase::Data),
		("RINGBUFFER_ENTRY", Keyspace::RINGBUFFER_ENTRY, Phase::Data),
		("GATE_VISIBILITY", Keyspace::GATE_VISIBILITY, Phase::Data),
		("DISTINCT_LAYOUT", Keyspace::DISTINCT_LAYOUT, Phase::Data),
		("RINGBUFFER_EXPIRY", Keyspace::RINGBUFFER_EXPIRY, Phase::Data),
		("RINGBUFFER_TTL_ARM", Keyspace::RINGBUFFER_TTL_ARM, Phase::Data),
		("SEAL_LEDGER", Keyspace::SEAL_LEDGER, Phase::Data),
		("JOIN_PUBLISHED", Keyspace::JOIN_PUBLISHED, Phase::Data),
		("JOIN_PIN", Keyspace::JOIN_PIN, Phase::Data),
		("FIRST_CUSTOM", Keyspace::FIRST_CUSTOM, Phase::Data),
	];

	/// Counts `Keyspace` constants from the source text. There is no reflection over associated
	/// constants, so this is the only way the census can notice a keyspace nobody listed.
	fn declared_keyspaces() -> usize {
		let source = include_str!("operator_group_state.rs");
		let body = source
			.split("impl Keyspace {")
			.nth(1)
			.expect("the Keyspace impl block is where the constants are declared");
		let body = body.split("\n}\n").next().expect("the impl block is closed");
		body.lines()
			.filter(|line| {
				let line = line.trim_start();
				line.starts_with("pub const") && line.contains("Self(")
			})
			.count()
	}

	#[test]
	fn a_bare_row_number_key_is_indistinguishable_from_another_groups_prefix() {
		// Row numbers and group ids come from independent node counters, so a singleton addressed by
		// a bare row number lands exactly on a live group's prefix and is erased when that group is
		// reclaimed - the operator reads a cold start, never an error.
		let mut bare = KeySerializer::with_capacity(4);
		bare.extend_u64(7u64);
		let bare = bare.finish().as_ref().to_vec();

		assert_eq!(bare, group_inner_prefix(GroupId(7)), "a bare row number encodes as a group prefix");
		assert!(
			contains(&group_identity_inner_range(GroupId(7)), &bare),
			"so reclaiming group 7 erases it with the row-number mappings"
		);
		assert!(!is_framed_inner(&bare));

		let framed = OperatorGroupStateKey::inner_encoded(
			GroupId::NODE_SCOPE,
			Keyspace::FIRST_CUSTOM,
			7u64.to_be_bytes(),
		);
		assert!(is_framed_inner(framed.as_slice()));
		assert!(
			!contains(&group_identity_inner_range(GroupId(7)), framed.as_slice()),
			"the framed form must sit outside every other group's range"
		);
	}

	#[test]
	fn the_empty_key_is_framing_because_it_sorts_below_every_group() {
		// Composed, an empty inner key is [kind][node] with nothing after it, so it sorts strictly
		// BELOW [kind][node][varint(group)] for every group: no reclaim phase can reach it, while a
		// node drop still prefix-covers it. A key with bytes lands in whatever group its varint spells.
		let empty: &[u8] = &[];
		assert!(is_framed_inner(empty));

		for group in GROUPS {
			let range = group_inner_range(GroupId(group));
			assert!(
				!contains(&range, empty),
				"the empty key must sit outside group {group}'s range, not merely be unattributed"
			);
		}
	}

	#[test]
	fn a_keyspace_this_substrate_never_defines_is_not_framing() {
		// A well-formed [group][keyspace] is as short as two bytes, so length alone is a weak test.
		// Demanding a keyspace that exists rejects the gap between the data range and the identity
		// constants; it cannot catch a key whose second field falls inside the data range.
		let mut stray = KeySerializer::with_capacity(4);
		stray.extend_u64(3u64).extend_u8(0x90u8);
		assert!(!is_framed_inner(stray.finish().as_ref()));

		for keyspace in DATA_KEYSPACES.iter().chain(IDENTITY_KEYSPACES.iter()) {
			assert!(
				is_framed_inner(OperatorGroupStateKey::inner_encoded(GroupId(3), *keyspace, []).as_slice()),
				"keyspace {keyspace:?} is one the substrate writes and must pass"
			);
		}
	}

	fn contains(range: &EncodedKeyRange, key: &[u8]) -> bool {
		let after_start = match &range.start {
			Bound::Included(start) => key >= start.as_slice(),
			Bound::Excluded(start) => key > start.as_slice(),
			Bound::Unbounded => true,
		};
		let before_end = match &range.end {
			Bound::Included(end) => key <= end.as_slice(),
			Bound::Excluded(end) => key < end.as_slice(),
			Bound::Unbounded => true,
		};
		after_start && before_end
	}

	fn population() -> Vec<OperatorGroupStateKey> {
		let mut keys = Vec::new();
		for node in NODES {
			for group in GROUPS {
				for keyspace in DATA_KEYSPACES.iter().chain(IDENTITY_KEYSPACES.iter()) {
					for coord in [0u64, 1, 999, u64::MAX] {
						keys.push(OperatorGroupStateKey::new(
							OperatorId(node),
							GroupId(group),
							*keyspace,
							coord.to_be_bytes().to_vec(),
						));
					}
				}
			}
			keys.push(OperatorGroupStateKey::node_scoped(
				OperatorId(node),
				Keyspace::GROUP_DICTIONARY,
				b"7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU".to_vec(),
			));
		}
		keys
	}

	#[test]
	fn a_group_range_contains_exactly_that_groups_keys() {
		// Erasing a group is a single bounded range delete, so completeness is structural rather than
		// a registry of keyspaces: a range that swallows a neighbour's keys destroys live state, and
		// one that misses its own leaks it.
		let population = population();
		for node in NODES {
			for group in GROUPS {
				let range = group_range(OperatorId(node), GroupId(group));
				for key in &population {
					let encoded = key.encode();
					let expected = key.node.0 == node && key.group.0 == group;
					assert_eq!(
						contains(&range, encoded.as_slice()),
						expected,
						"node {node} group {group} range disagreed about a key of node {} \
						 group {}",
						key.node.0,
						key.group.0
					);
				}
			}
		}
	}

	#[test]
	fn variable_length_group_ids_cannot_prefix_one_another() {
		// Group ids are varint encoded, so different magnitudes give different byte lengths. If a
		// short id's encoding were a prefix of a longer one's, group 1's range would contain group
		// 1000's keys and reclaiming the former would erase the latter.
		let encodings: Vec<Vec<u8>> = GROUPS
			.iter()
			.map(|group| {
				OperatorGroupStateKey::new(OperatorId(1), GroupId(*group), Keyspace::ACCUMULATOR, vec![])
					.encode()
					.as_slice()
					.to_vec()
			})
			.collect();

		for (i, a) in encodings.iter().enumerate() {
			for (j, b) in encodings.iter().enumerate() {
				if i != j {
					assert!(
						!b.starts_with(a.as_slice()),
						"group {} encodes as a prefix of group {}",
						GROUPS[i],
						GROUPS[j]
					);
				}
			}
		}
	}

	#[test]
	fn the_data_and_identity_ranges_partition_the_group() {
		// Two-phase reclamation is one range operation per phase only because the keyspace byte orders
		// identity before data after inversion. A leak either way takes the row-number mapping with
		// phase 1 (duplicate rows on the next wake) or leaves live accumulators behind.
		for node in NODES {
			for group in GROUPS {
				let data = group_data_range(OperatorId(node), GroupId(group));
				let identity = group_identity_range(OperatorId(node), GroupId(group));

				for keyspace in DATA_KEYSPACES {
					let key = OperatorGroupStateKey::new(
						OperatorId(node),
						GroupId(group),
						keyspace,
						vec![7, 7],
					)
					.encode();
					assert!(
						contains(&data, key.as_slice()),
						"data keyspace {keyspace:?} must fall in the phase-1 range"
					);
					assert!(
						!contains(&identity, key.as_slice()),
						"data keyspace {keyspace:?} must not fall in the phase-2 range"
					);
				}

				for keyspace in IDENTITY_KEYSPACES {
					let key = OperatorGroupStateKey::new(
						OperatorId(node),
						GroupId(group),
						keyspace,
						vec![7, 7],
					)
					.encode();
					assert!(
						contains(&identity, key.as_slice()),
						"identity keyspace {keyspace:?} must fall in the phase-2 range"
					);
					assert!(
						!contains(&data, key.as_slice()),
						"identity keyspace {keyspace:?} must survive phase 1"
					);
				}
			}
		}
	}

	#[test]
	fn every_declared_keyspace_is_distinct_framing_and_swept_by_exactly_one_phase() {
		// The partition test samples six keyspaces of thirty-six, so it cannot see a duplicate byte
		// (two operators' state at one address, each erasing the other) or a keyspace above
		// HIGHEST_DATA missing from `is_known` (the sweep panics on the first row it scans there).
		assert_eq!(
			CENSUS.len(),
			declared_keyspaces(),
			"a keyspace was added to Keyspace without being added to the census, so nothing below \
			 ever looks at its byte"
		);

		let mut seen: Vec<(&str, u8)> = Vec::new();
		for (name, keyspace, phase) in CENSUS {
			if let Some((other, _)) = seen.iter().find(|(_, byte)| *byte == keyspace.0) {
				panic!("{name} and {other} both claim keyspace byte {:#04x}", keyspace.0);
			}
			seen.push((name, keyspace.0));

			assert!(
				keyspace.is_known(),
				"{name} is declared but not framing, so the sweep panics on the first row it holds"
			);

			let key = OperatorGroupStateKey::new(OperatorId(9), GroupId(4), keyspace, vec![7, 7]).encode();
			let data = contains(&group_data_range(OperatorId(9), GroupId(4)), key.as_slice());
			let identity = contains(&group_identity_range(OperatorId(9), GroupId(4)), key.as_slice());

			assert!(data != identity, "{name} must fall in exactly one phase, not {data} and {identity}");
			assert_eq!(
				data,
				phase == Phase::Data,
				"{name} is declared {phase:?} but the phase-1 range says data={data}"
			);
			assert_eq!(
				keyspace.is_data(),
				phase == Phase::Data,
				"{name} is declared {phase:?} but is_data says {}",
				keyspace.is_data()
			);
		}
	}

	#[test]
	fn node_scoped_entries_sit_outside_every_group_range() {
		// Reclaiming a group must not touch the node-scope table that resolves group ids, or the
		// substrate erases its own address book while other groups still depend on it.
		for node in NODES {
			let dictionary = OperatorGroupStateKey::node_scoped(
				OperatorId(node),
				Keyspace::GROUP_DICTIONARY,
				b"mint-pubkey".to_vec(),
			)
			.encode();
			for group in GROUPS {
				let range = group_range(OperatorId(node), GroupId(group));
				assert!(
					!contains(&range, dictionary.as_slice()),
					"group {group} range must not contain the node-scope dictionary"
				);
			}
		}
	}

	#[test]
	fn a_node_range_contains_exactly_that_nodes_keys() {
		// drop_operator erases a whole node by range: containing a neighbour's keys destroys a live
		// flow's state, and missing its own strands the keyspace.
		let population = population();
		for node in NODES {
			let range = node_range(OperatorId(node));
			for key in &population {
				let encoded = key.encode();
				assert_eq!(
					contains(&range, encoded.as_slice()),
					key.node.0 == node,
					"node {node} range disagreed about a key of node {}",
					key.node.0
				);
			}
		}
	}

	#[test]
	fn a_keyspace_range_isolates_one_keyspace_of_one_group() {
		// Hydration and per-keyspace scans read through this range; bleeding into an adjacent keyspace
		// feeds one cache another's payloads - a decode failure at best, mixed state at worst.
		let node = OperatorId(17);
		let group = GroupId(42);
		let range = keyspace_range(node, group, Keyspace::BUFFER);

		let inside = OperatorGroupStateKey::new(node, group, Keyspace::BUFFER, vec![1]).encode();
		assert!(contains(&range, inside.as_slice()));

		for other in [Keyspace::ACCUMULATOR, Keyspace::RUNNING, Keyspace::GROUP_RECORD] {
			let key = OperatorGroupStateKey::new(node, group, other, vec![1]).encode();
			assert!(!contains(&range, key.as_slice()), "keyspace {other:?} leaked into the buffer range");
		}

		let other_group = OperatorGroupStateKey::new(node, GroupId(43), Keyspace::BUFFER, vec![1]).encode();
		assert!(!contains(&range, other_group.as_slice()), "another group's buffer leaked into the range");
	}

	#[test]
	fn encode_decode_round_trips_every_component() {
		let key = OperatorGroupStateKey::new(
			OperatorId(0xDEAD_BEEF),
			GroupId(123_456),
			Keyspace::FIRST_CUSTOM,
			vec![1, 2, 3, 4],
		);
		assert_eq!(OperatorGroupStateKey::decode(&key.encode()), Some(key));
	}

	#[test]
	fn keys_still_classify_as_operator_state_of_their_node() {
		// Tier classification and the CDC exclusion both key off KeyKind::OperatorState. A key that
		// stopped classifying as Operator would be routed to the wrong tier and appear in the CDC
		// log, which operator state must never do.
		let key = OperatorGroupStateKey::new(OperatorId(9), GroupId(4), Keyspace::ACCUMULATOR, vec![1]).encode();

		assert_eq!(classify_key(&key), EntryKind::Operator(OperatorId(9)));

		let legacy = OperatorStateKey::decode(&key).expect("must remain decodable as its key kind");
		assert_eq!(legacy.node, OperatorId(9));
	}

	#[test]
	fn an_inner_key_composed_with_its_node_prefix_reproduces_the_full_key() {
		// The state API owns the [kind][node] head and callers supply only the tail. If the two forms
		// drifted, a key written through the state API would be unreachable from a range built by the
		// full-key helpers - state stranded where reclamation cannot see it.
		let key = OperatorGroupStateKey::new(OperatorId(17), GroupId(42), Keyspace::BUFFER, vec![9, 9]);

		let mut composed = OperatorStateKey::encoded(OperatorId(17), vec![]).as_slice().to_vec();
		composed.extend_from_slice(key.inner().as_slice());

		assert_eq!(composed, key.encode().as_slice(), "inner key plus node prefix must equal the full key");
	}

	#[test]
	fn the_node_scope_group_range_stays_inside_its_node() {
		// Group 0 encodes as 0xFF, so its inner prefix is all-ones and has no byte-wise successor:
		// EncodedKeyRange::prefix yields an unbounded end. Composing the node prefix bounds it to the
		// rest of this node, which is exactly group 0's keys because it sorts last within the node.
		let range = group_inner_range(GroupId::NODE_SCOPE)
			.with_prefix(OperatorStateKey::encoded(OperatorId(17), vec![]));

		let own = OperatorGroupStateKey::node_scoped(OperatorId(17), Keyspace::GROUP_DICTIONARY, vec![1]).encode();
		assert!(contains(&range, own.as_slice()), "the node's own dictionary entry must be in range");

		for node in NODES {
			if node == 17 {
				continue;
			}
			for keyspace in [Keyspace::GROUP_DICTIONARY, Keyspace::ACCUMULATOR] {
				let foreign =
					OperatorGroupStateKey::new(OperatorId(node), GroupId::NODE_SCOPE, keyspace, vec![1])
						.encode();
				assert!(
					!contains(&range, foreign.as_slice()),
					"node {node} leaked into node 17's node-scope range"
				);
			}
		}
	}

	#[test]
	fn inner_ranges_partition_the_group_like_their_full_key_counterparts() {
		// Reclamation runs through the state API, so the inner forms are the ones that execute; a
		// split holding only for full keys would still take the row-number mapping with phase 1.
		let node = OperatorId(17);
		let prefix = OperatorStateKey::encoded(node, vec![]);
		for group in GROUPS {
			let data = group_data_inner_range(GroupId(group)).with_prefix(prefix.clone());
			let identity = group_identity_inner_range(GroupId(group)).with_prefix(prefix.clone());

			for keyspace in DATA_KEYSPACES {
				let key = OperatorGroupStateKey::new(node, GroupId(group), keyspace, vec![7]).encode();
				assert!(contains(&data, key.as_slice()));
				assert!(!contains(&identity, key.as_slice()));
			}
			for keyspace in IDENTITY_KEYSPACES {
				let key = OperatorGroupStateKey::new(node, GroupId(group), keyspace, vec![7]).encode();
				assert!(contains(&identity, key.as_slice()));
				assert!(!contains(&data, key.as_slice()));
			}
		}
	}

	#[test]
	fn decode_inner_round_trips_the_tail() {
		let key = OperatorGroupStateKey::new(OperatorId(3), GroupId(77), Keyspace::EMIT, vec![4, 5, 6]);
		let (group, keyspace, suffix) =
			OperatorGroupStateKey::decode_inner(key.inner().as_slice()).expect("inner must decode");

		assert_eq!(group, GroupId(77));
		assert_eq!(keyspace, Keyspace::EMIT);
		assert_eq!(suffix, vec![4, 5, 6]);
	}

	#[test]
	fn interned_group_keys_stay_compact() {
		// Interning buys contiguity without paying for it in key bytes; raw group bytes would put two
		// base58 addresses in every accumulator key.
		let interned =
			OperatorGroupStateKey::new(OperatorId(17), GroupId(123_456), Keyspace::ACCUMULATOR, vec![0; 8])
				.encode();
		let raw_group_bytes = b"7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU\
		                        So11111111111111111111111111111111111111112"
			.len() + 8;

		assert!(
			interned.as_slice().len() * 4 < raw_group_bytes,
			"an interned state key ({} bytes) must stay far below a raw-group key ({} bytes)",
			interned.as_slice().len(),
			raw_group_bytes
		);
	}

	#[test]
	fn the_ram_predicate_and_the_disk_range_agree_on_every_key() {
		// Phase 1 deletes disk rows by range and drops cached rows by predicate. A key the range takes
		// but the predicate rejects becomes a ghost row served from RAM; a key the predicate takes but
		// the range leaves clears the membership bit for a row still on disk - silent loss.
		for group in GROUPS.map(GroupId) {
			let range = group_data_inner_range(group);
			for other in GROUPS.map(GroupId) {
				for keyspace in DATA_KEYSPACES.iter().chain(IDENTITY_KEYSPACES.iter()) {
					let key = OperatorGroupStateKey::inner_encoded(other, *keyspace, vec![7, 7]);
					let in_range = contains(&range, key.as_slice());
					let in_predicate = group_data_of_inner(key.as_slice()) == Some(group);
					assert_eq!(
						in_range, in_predicate,
						"disk range and RAM predicate disagree for group {group:?} on a \
						 {keyspace:?} key of group {other:?}"
					);
				}
			}
		}
	}

	#[test]
	fn the_ram_predicate_refuses_identity_keyspaces() {
		// An identity row's disk row deliberately outlives the data so a sink row can still name its
		// mapping; dropping the cached copy would clear the membership bit for a stored key.
		for keyspace in IDENTITY_KEYSPACES {
			let key = OperatorGroupStateKey::inner_encoded(GroupId(9), keyspace, vec![1]);
			assert_eq!(
				group_data_of_inner(key.as_slice()),
				None,
				"{keyspace:?} must not be reported as reclaimable group data"
			);
		}
	}

	#[test]
	fn a_key_too_short_to_carry_a_keyspace_is_refused() {
		// The group id is a varint, so there is no length floor: two bytes is a legitimate key for a
		// small group with an empty suffix. Only a key that cannot yield both fields is undecodable.
		assert_eq!(group_data_of_inner(&[]), None);
		assert_eq!(group_data_of_inner(&[0xAB]), None, "a group with no keyspace byte must not decode");
	}

	#[test]
	fn the_predicate_agrees_with_the_disk_range_on_arbitrary_bytes() {
		// Cached keys arrive as opaque bytes from operator code, so the predicate and the range must
		// also agree on strings no encoder produced.
		let mut seed = 0x2545F4914F6CDD1Du64;
		let mut next = move || {
			seed ^= seed << 13;
			seed ^= seed >> 7;
			seed ^= seed << 17;
			seed
		};

		for _ in 0..2000 {
			let len = (next() % 12) as usize;
			let key: Vec<u8> = (0..len).map(|_| (next() % 256) as u8).collect();
			let Some(group) = group_data_of_inner(&key) else {
				continue;
			};
			assert!(
				contains(&group_data_inner_range(group), &key),
				"predicate attributed {key:?} to {group:?} but the disk range excludes it"
			);
		}
	}

	#[test]
	fn a_group_set_is_sorted_deduped_and_never_admits_node_scope() {
		// The set is searched per cached key, so ordering is a precondition for binary_search. Node
		// scope holds the interning dictionary and must never be reachable through bulk invalidation.
		let set = GroupSet::new([GroupId(9), GroupId(2), GroupId(9), GroupId::NODE_SCOPE, GroupId(5)]);

		assert_eq!(set.as_slice(), &[GroupId(2), GroupId(5), GroupId(9)]);
		assert_eq!(set.len(), 3);
		assert!(set.contains(GroupId(5)));
		assert!(!set.contains(GroupId(3)));
		assert!(!set.contains(GroupId::NODE_SCOPE), "node scope must be filtered out, not merely unsorted");
	}

	#[test]
	fn an_empty_group_set_matches_nothing() {
		let set = GroupSet::new([]);

		assert!(set.is_empty());
		assert!(!set.contains(GroupId::FIRST));
	}

	#[test]
	fn a_group_set_hands_the_ffi_boundary_a_plain_u64_array() {
		// GroupId is repr(transparent) so the vtable entry can take the slice as *const u64 with no
		// marshalling copy. If the repr ever changes, this reads the wrong bytes across the dylib.
		let set = GroupSet::new([GroupId(3), GroupId(1), GroupId(2)]);
		let (ptr, len) = set.as_raw_parts();

		assert_eq!(len, 3);
		// SAFETY: the slice is alive for the whole assertion and GroupId is repr(transparent) over u64.
		let raw = unsafe { slice::from_raw_parts(ptr, len) };
		assert_eq!(raw, &[1u64, 2, 3]);
	}
}
