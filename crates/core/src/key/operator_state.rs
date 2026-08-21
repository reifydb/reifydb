// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encode_u8,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};

use super::{EncodableKey, KeyKind};
use crate::interface::catalog::flow::OperatorId;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(pub u64);

impl GroupId {
	pub const ROOT: Self = Self(0);

	pub const FIRST: Self = Self(1);

	pub fn is_root(&self) -> bool {
		*self == Self::ROOT
	}
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupSet(Vec<GroupId>);

impl GroupSet {
	pub fn new(groups: impl IntoIterator<Item = GroupId>) -> Self {
		let mut groups: Vec<GroupId> = groups.into_iter().filter(|g| !g.is_root()).collect();
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

	pub const SOURCE_WATERMARK: Self = Self(0xFA);

	pub const TIMER_WHEEL: Self = Self(0xF9);

	pub const TIMER_INDEX: Self = Self(0xF8);

	pub const APPEND_DICTIONARY: Self = Self(0xF7);

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

	pub const RINGBUFFER_META: Self = Self(0x29);

	pub const REAP_QUEUE: Self = Self(0x2A);

	pub const SEAL_ANCHOR: Self = Self(0x2B);

	pub const CUSTOM: Self = Self(0x40);

	pub fn name(&self) -> &'static str {
		match *self {
			Self::ROW_NUMBER_MAPPING => "ROW_NUMBER_MAPPING",
			Self::GROUP_DICTIONARY => "GROUP_DICTIONARY",
			Self::NODE_COUNTER => "NODE_COUNTER",
			Self::GROUP_RECORD => "GROUP_RECORD",
			Self::SOURCE_WATERMARK => "SOURCE_WATERMARK",
			Self::TIMER_WHEEL => "TIMER_WHEEL",
			Self::TIMER_INDEX => "TIMER_INDEX",
			Self::APPEND_DICTIONARY => "APPEND_DICTIONARY",
			Self::ACCUMULATOR => "ACCUMULATOR",
			Self::BUFFER => "BUFFER",
			Self::RUNNING => "RUNNING",
			Self::EMIT => "EMIT",
			Self::EXPIRY => "EXPIRY",
			Self::COUNT => "COUNT",
			Self::ROW_INDEX => "ROW_INDEX",
			Self::SESSION => "SESSION",
			Self::ROLLING_META => "ROLLING_META",
			Self::ENGINE_META => "ENGINE_META",
			Self::DISTINCT_ENTRY => "DISTINCT_ENTRY",
			Self::WINDOW_META => "WINDOW_META",
			Self::JOIN_LEFT => "JOIN_LEFT",
			Self::JOIN_RIGHT => "JOIN_RIGHT",
			Self::JOIN_SCHEMA => "JOIN_SCHEMA",
			Self::RINGBUFFER_FORWARD => "RINGBUFFER_FORWARD",
			Self::RINGBUFFER_ENTRY => "RINGBUFFER_ENTRY",
			Self::GATE_VISIBILITY => "GATE_VISIBILITY",
			Self::DISTINCT_LAYOUT => "DISTINCT_LAYOUT",
			Self::RINGBUFFER_EXPIRY => "RINGBUFFER_EXPIRY",
			Self::RINGBUFFER_TTL_ARM => "RINGBUFFER_TTL_ARM",
			Self::SEAL_LEDGER => "SEAL_LEDGER",
			Self::JOIN_PUBLISHED => "JOIN_PUBLISHED",
			Self::JOIN_PIN => "JOIN_PIN",
			Self::RINGBUFFER_META => "RINGBUFFER_META",
			Self::REAP_QUEUE => "REAP_QUEUE",
			Self::SEAL_ANCHOR => "SEAL_ANCHOR",
			Self::CUSTOM => "CUSTOM",
			_ => "CUSTOM",
		}
	}

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
					| Self::GROUP_RECORD | Self::SOURCE_WATERMARK
					| Self::TIMER_WHEEL | Self::TIMER_INDEX
					| Self::APPEND_DICTIONARY
			)
	}
}

pub fn is_framed_inner(inner: &[u8]) -> bool {
	inner.is_empty() || OperatorStateKey::decode_inner(inner).is_some_and(|(_, keyspace, _)| keyspace.is_known())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorStateKey {
	pub operator: OperatorId,
	pub group: GroupId,
	pub keyspace: Keyspace,
	pub suffix: Vec<u8>,
}

impl OperatorStateKey {
	pub fn new(operator: OperatorId, group: GroupId, keyspace: Keyspace, suffix: impl Into<Vec<u8>>) -> Self {
		Self {
			operator,
			group,
			keyspace,
			suffix: suffix.into(),
		}
	}

	pub fn root(operator: OperatorId, keyspace: Keyspace, suffix: impl Into<Vec<u8>>) -> Self {
		Self::new(operator, GroupId::ROOT, keyspace, suffix)
	}

	pub fn encoded(
		operator: OperatorId,
		group: GroupId,
		keyspace: Keyspace,
		suffix: impl AsRef<[u8]>,
	) -> EncodedKey {
		let suffix = suffix.as_ref();
		let mut serializer = KeySerializer::with_capacity(20 + suffix.len());
		serializer
			.extend_u8(KeyKind::OperatorState as u8)
			.extend_u64(operator.0)
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

	pub const KEYSPACE_INNER_OFFSET: u32 = size_of::<u64>() as u32;

	pub fn decode_keyspace(stored: u8) -> Keyspace {
		Keyspace(KeyDeserializer::from_bytes(&[stored]).read_u8().expect("a single byte decodes as u8"))
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

	pub fn node_range(operator: OperatorId) -> EncodedKeyRange {
		node_range(operator)
	}

	pub fn decode_operator(key: &EncodedKey) -> Option<(OperatorId, EncodedKey)> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());
		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != KeyKind::OperatorState {
			return None;
		}
		let operator = de.read_u64().ok()?;
		let inner = de.read_raw(de.remaining()).ok()?.to_vec();
		Some((OperatorId(operator), EncodedKey::new(inner)))
	}
}

impl EncodableKey for OperatorStateKey {
	const KIND: KeyKind = KeyKind::OperatorState;

	fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(20 + self.suffix.len());
		serializer
			.extend_u8(KeyKind::OperatorState as u8)
			.extend_u64(self.operator.0)
			.extend_u64(self.group.0)
			.extend_u8(self.keyspace.0)
			.extend_raw(&self.suffix);
		serializer.to_encoded_key()
	}

	fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != KeyKind::OperatorState {
			return None;
		}

		let operator = de.read_u64().ok()?;
		let group = de.read_u64().ok()?;
		let keyspace = de.read_u8().ok()?;
		let suffix = de.read_raw(de.remaining()).ok()?.to_vec();

		Some(Self {
			operator: OperatorId(operator),
			group: GroupId(group),
			keyspace: Keyspace(keyspace),
			suffix,
		})
	}
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupStateKey(EncodedKey);

impl GroupStateKey {
	pub fn new(group: GroupId, keyspace: Keyspace, suffix: impl AsRef<[u8]>) -> Self {
		OperatorStateKey::inner_encoded(group, keyspace, suffix)
	}

	pub fn root(keyspace: Keyspace, suffix: impl AsRef<[u8]>) -> Self {
		Self::new(GroupId::ROOT, keyspace, suffix)
	}

	pub fn from_framed(key: EncodedKey) -> Option<Self> {
		is_framed_inner(key.as_slice()).then_some(Self(key))
	}

	pub fn bound_unchecked(key: EncodedKey) -> Self {
		Self(key)
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
		OperatorStateKey::decode_inner(self.0.as_slice()).map(|(group, _, _)| group)
	}

	pub fn keyspace(&self) -> Option<Keyspace> {
		let bytes = self.0.as_slice();
		let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
		(bytes.len() > offset).then(|| Keyspace(encode_u8(bytes[offset])))
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

pub fn keyspace_inner_range_upto(group: GroupId, keyspace: Keyspace, suffix: &[u8]) -> EncodedKeyRange {
	let mut bound = keyspace_inner_prefix(group, keyspace);
	bound.extend_from_slice(suffix);
	EncodedKeyRange::new(keyspace_inner_range(group, keyspace).start, EncodedKeyRange::prefix(&bound).end)
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

pub fn node_prefix(operator: OperatorId) -> Vec<u8> {
	let mut serializer = KeySerializer::with_capacity(12);
	serializer.extend_u8(KeyKind::OperatorState as u8).extend_u64(operator.0);
	serializer.finish().as_ref().to_vec()
}

fn group_prefix(operator: OperatorId, group: GroupId) -> Vec<u8> {
	let mut serializer = KeySerializer::with_capacity(20);
	serializer.extend_u8(KeyKind::OperatorState as u8).extend_u64(operator.0).extend_u64(group.0);
	serializer.finish().as_ref().to_vec()
}

fn keyspace_prefix(operator: OperatorId, group: GroupId, keyspace: Keyspace) -> Vec<u8> {
	let mut prefix = group_prefix(operator, group);
	prefix.push(encode_u8(keyspace.0));
	prefix
}

pub fn node_range(operator: OperatorId) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&node_prefix(operator))
}

pub fn group_range(operator: OperatorId, group: GroupId) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&group_prefix(operator, group))
}

pub fn keyspace_range(operator: OperatorId, group: GroupId, keyspace: Keyspace) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&keyspace_prefix(operator, group, keyspace))
}

pub fn group_data_range(operator: OperatorId, group: GroupId) -> EncodedKeyRange {
	let prefix = group_prefix(operator, group);
	let mut start = prefix.clone();
	start.push(encode_u8(Keyspace::HIGHEST_DATA));
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(start)), EncodedKeyRange::prefix(&prefix).end)
}

pub fn group_identity_range(operator: OperatorId, group: GroupId) -> EncodedKeyRange {
	let prefix = group_prefix(operator, group);
	let mut end = prefix.clone();
	end.push(encode_u8(Keyspace::HIGHEST_DATA));
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(prefix)), Bound::Excluded(EncodedKey::new(end)))
}

#[cfg(test)]
mod tests {
	use std::{ops::Bound, slice};

	use super::{
		EncodedKey, EncodedKeyRange, GroupId, GroupSet, KeySerializer, Keyspace, OperatorStateKey,
		group_data_inner_range, group_data_of_inner, group_data_range, group_identity_inner_range,
		group_identity_range, group_inner_prefix, group_inner_range, group_range, is_framed_inner,
		keyspace_range, node_prefix, node_range,
	};
	use crate::{interface::catalog::flow::OperatorId, key::EncodableKey};

	const NODES: [u64; 4] = [1, 17, 300, 70_000];
	const GROUPS: [u64; 8] = [1, 2, 127, 128, 1000, 100_000, 1 << 30, u64::MAX];
	const DATA_KEYSPACES: [Keyspace; 4] =
		[Keyspace::ACCUMULATOR, Keyspace::BUFFER, Keyspace::RUNNING, Keyspace::CUSTOM];
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
		("APPEND_DICTIONARY", Keyspace::APPEND_DICTIONARY, Phase::Identity),
		("GROUP_DICTIONARY", Keyspace::GROUP_DICTIONARY, Phase::Identity),
		("NODE_COUNTER", Keyspace::NODE_COUNTER, Phase::Identity),
		("GROUP_RECORD", Keyspace::GROUP_RECORD, Phase::Identity),
		("SOURCE_WATERMARK", Keyspace::SOURCE_WATERMARK, Phase::Identity),
		("TIMER_WHEEL", Keyspace::TIMER_WHEEL, Phase::Identity),
		("TIMER_INDEX", Keyspace::TIMER_INDEX, Phase::Identity),
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
		("RINGBUFFER_META", Keyspace::RINGBUFFER_META, Phase::Data),
		("REAP_QUEUE", Keyspace::REAP_QUEUE, Phase::Data),
		("SEAL_ANCHOR", Keyspace::SEAL_ANCHOR, Phase::Data),
		("CUSTOM", Keyspace::CUSTOM, Phase::Data),
	];

	/// Counts `Keyspace` constants from the source text. There is no reflection over associated
	/// constants, so this is the only way the census can notice a keyspace nobody listed.
	fn declared_keyspaces() -> usize {
		let source = include_str!("operator_state.rs");
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
		// a bare row number equals a group prefix, so it is erased with that group on reclaim, never errors
		let mut bare = KeySerializer::with_capacity(4);
		bare.extend_u64(7u64);
		let bare = bare.finish().as_ref().to_vec();

		assert_eq!(bare, group_inner_prefix(GroupId(7)), "a bare row number encodes as a group prefix");
		assert!(
			contains(&group_identity_inner_range(GroupId(7)), &bare),
			"so reclaiming group 7 erases it with the row-number mappings"
		);
		assert!(!is_framed_inner(&bare));

		let framed = OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::CUSTOM, 7u64.to_be_bytes());
		assert!(is_framed_inner(framed.as_slice()));
		assert!(
			!contains(&group_identity_inner_range(GroupId(7)), framed.as_slice()),
			"the framed form must sit outside every other group's range"
		);
	}

	#[test]
	fn the_empty_key_is_framing_because_it_sorts_below_every_group() {
		// an empty inner key must sort below every group's prefix, or a reclaim phase could reach it
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
		// a two-byte group+keyspace pair must not be framing unless the keyspace is one the substrate declares
		let mut stray = KeySerializer::with_capacity(4);
		stray.extend_u64(3u64).extend_u8(0x90u8);
		assert!(!is_framed_inner(stray.finish().as_ref()));

		for keyspace in DATA_KEYSPACES.iter().chain(IDENTITY_KEYSPACES.iter()) {
			assert!(
				is_framed_inner(OperatorStateKey::inner_encoded(GroupId(3), *keyspace, []).as_slice()),
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

	fn population() -> Vec<OperatorStateKey> {
		let mut keys = Vec::new();
		for operator in NODES {
			for group in GROUPS {
				for keyspace in DATA_KEYSPACES.iter().chain(IDENTITY_KEYSPACES.iter()) {
					for coord in [0u64, 1, 999, u64::MAX] {
						keys.push(OperatorStateKey::new(
							OperatorId(operator),
							GroupId(group),
							*keyspace,
							coord.to_be_bytes().to_vec(),
						));
					}
				}
			}
			keys.push(OperatorStateKey::root(
				OperatorId(operator),
				Keyspace::GROUP_DICTIONARY,
				b"7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU".to_vec(),
			));
		}
		keys
	}

	#[test]
	fn a_group_range_contains_exactly_that_groups_keys() {
		// a group range must contain exactly that operator+group's keys, or reclaim destroys or leaks state
		let population = population();
		for operator in NODES {
			for group in GROUPS {
				let range = group_range(OperatorId(operator), GroupId(group));
				for key in &population {
					let encoded = key.encode();
					let expected = key.operator.0 == operator && key.group.0 == group;
					assert_eq!(
						contains(&range, encoded.as_slice()),
						expected,
						"operator {operator} group {group} range disagreed about a key of operator {} \
						 group {}",
						key.operator.0,
						key.group.0
					);
				}
			}
		}
	}

	#[test]
	fn variable_length_group_ids_cannot_prefix_one_another() {
		// no group id's varint encoding may prefix another's, or reclaiming the shorter erases the longer's
		// keys
		let encodings: Vec<Vec<u8>> = GROUPS
			.iter()
			.map(|group| {
				OperatorStateKey::new(OperatorId(1), GroupId(*group), Keyspace::ACCUMULATOR, vec![])
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
		// data and identity keyspaces must fall in exactly one reclamation phase, never both or neither
		for operator in NODES {
			for group in GROUPS {
				let data = group_data_range(OperatorId(operator), GroupId(group));
				let identity = group_identity_range(OperatorId(operator), GroupId(group));

				for keyspace in DATA_KEYSPACES {
					let key = OperatorStateKey::new(
						OperatorId(operator),
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
					let key = OperatorStateKey::new(
						OperatorId(operator),
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
	fn every_declared_keyspace_names_itself_for_offline_attribution() {
		// every declared keyspace must name itself, or an offline census misattributes it as CUSTOM
		for (name, keyspace, _) in CENSUS {
			assert_eq!(
				keyspace.name(),
				name,
				"{name} ({:#04x}) does not name itself, so an offline census reports it as CUSTOM",
				keyspace.0
			);
		}

		assert_eq!(
			Keyspace(0x41).name(),
			"CUSTOM",
			"a byte no constant claims must fall through rather than borrow a neighbour's name"
		);
	}

	#[test]
	fn every_declared_keyspace_is_distinct_framing_and_swept_by_exactly_one_phase() {
		// every declared keyspace must have a unique byte and belong to exactly one reclamation phase
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

			let key = OperatorStateKey::new(OperatorId(9), GroupId(4), keyspace, vec![7, 7]).encode();
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
	fn root_entries_sit_outside_every_group_range() {
		// the root-scoped dictionary entry must sit outside every group range, or reclaiming a group erases it
		for operator in NODES {
			let dictionary = OperatorStateKey::root(
				OperatorId(operator),
				Keyspace::GROUP_DICTIONARY,
				b"mint-pubkey".to_vec(),
			)
			.encode();
			for group in GROUPS {
				let range = group_range(OperatorId(operator), GroupId(group));
				assert!(
					!contains(&range, dictionary.as_slice()),
					"group {group} range must not contain the root group's dictionary entry"
				);
			}
		}
	}

	#[test]
	fn a_node_range_contains_exactly_that_nodes_keys() {
		// a node range must contain exactly its own operator's keys, since drop_operator deletes by range
		let population = population();
		for operator in NODES {
			let range = node_range(OperatorId(operator));
			for key in &population {
				let encoded = key.encode();
				assert_eq!(
					contains(&range, encoded.as_slice()),
					key.operator.0 == operator,
					"operator {operator} range disagreed about a key of operator {}",
					key.operator.0
				);
			}
		}
	}

	#[test]
	fn a_keyspace_range_isolates_one_keyspace_of_one_group() {
		// a keyspace range must isolate exactly one keyspace of one group, or scans mix incompatible payloads
		let operator = OperatorId(17);
		let group = GroupId(42);
		let range = keyspace_range(operator, group, Keyspace::BUFFER);

		let inside = OperatorStateKey::new(operator, group, Keyspace::BUFFER, vec![1]).encode();
		assert!(contains(&range, inside.as_slice()));

		for other in [Keyspace::ACCUMULATOR, Keyspace::RUNNING, Keyspace::GROUP_RECORD] {
			let key = OperatorStateKey::new(operator, group, other, vec![1]).encode();
			assert!(!contains(&range, key.as_slice()), "keyspace {other:?} leaked into the buffer range");
		}

		let other_group = OperatorStateKey::new(operator, GroupId(43), Keyspace::BUFFER, vec![1]).encode();
		assert!(!contains(&range, other_group.as_slice()), "another group's buffer leaked into the range");
	}

	#[test]
	fn encode_decode_round_trips_every_component() {
		let key = OperatorStateKey::new(
			OperatorId(0xDEAD_BEEF),
			GroupId(123_456),
			Keyspace::CUSTOM,
			vec![1, 2, 3, 4],
		);
		assert_eq!(OperatorStateKey::decode(&key.encode()), Some(key));
	}

	#[test]
	fn keys_still_decode_as_operator_state_of_their_node() {
		// every key of this kind must still decode to its own operator, or state is misrouted into the CDC log
		let key = OperatorStateKey::new(OperatorId(9), GroupId(4), Keyspace::ACCUMULATOR, vec![1]).encode();

		let decoded = OperatorStateKey::decode(&key).expect("must remain decodable as its key kind");
		assert_eq!(decoded.operator, OperatorId(9));
	}

	#[test]
	fn an_inner_key_composed_with_its_node_prefix_reproduces_the_full_key() {
		// inner key plus node prefix must reproduce the full key, or state written through the API is
		// unreachable
		let key = OperatorStateKey::new(OperatorId(17), GroupId(42), Keyspace::BUFFER, vec![9, 9]);

		let mut composed = node_prefix(OperatorId(17));
		composed.extend_from_slice(key.inner().as_slice());

		assert_eq!(composed, key.encode().as_slice(), "inner key plus operator prefix must equal the full key");
	}

	#[test]
	fn the_root_group_range_stays_inside_its_node() {
		// group 0's inner range has no byte-wise successor, so it must stay bounded by the operator prefix
		let range = group_inner_range(GroupId::ROOT).with_prefix(EncodedKey::new(node_prefix(OperatorId(17))));

		let own = OperatorStateKey::root(OperatorId(17), Keyspace::GROUP_DICTIONARY, vec![1]).encode();
		assert!(contains(&range, own.as_slice()), "the operator's own dictionary entry must be in range");

		for operator in NODES {
			if operator == 17 {
				continue;
			}
			for keyspace in [Keyspace::GROUP_DICTIONARY, Keyspace::ACCUMULATOR] {
				let foreign =
					OperatorStateKey::new(OperatorId(operator), GroupId::ROOT, keyspace, vec![1])
						.encode();
				assert!(
					!contains(&range, foreign.as_slice()),
					"operator {operator} leaked into operator 17's root-group range"
				);
			}
		}
	}

	#[test]
	fn inner_ranges_partition_the_group_like_their_full_key_counterparts() {
		// inner data/identity ranges must partition a group like their full-key counterparts, since reclamation
		// uses them
		let operator = OperatorId(17);
		let prefix = EncodedKey::new(node_prefix(operator));
		for group in GROUPS {
			let data = group_data_inner_range(GroupId(group)).with_prefix(prefix.clone());
			let identity = group_identity_inner_range(GroupId(group)).with_prefix(prefix.clone());

			for keyspace in DATA_KEYSPACES {
				let key = OperatorStateKey::new(operator, GroupId(group), keyspace, vec![7]).encode();
				assert!(contains(&data, key.as_slice()));
				assert!(!contains(&identity, key.as_slice()));
			}
			for keyspace in IDENTITY_KEYSPACES {
				let key = OperatorStateKey::new(operator, GroupId(group), keyspace, vec![7]).encode();
				assert!(contains(&identity, key.as_slice()));
				assert!(!contains(&data, key.as_slice()));
			}
		}
	}

	#[test]
	fn decode_inner_round_trips_the_tail() {
		let key = OperatorStateKey::new(OperatorId(3), GroupId(77), Keyspace::EMIT, vec![4, 5, 6]);
		let (group, keyspace, suffix) =
			OperatorStateKey::decode_inner(key.inner().as_slice()).expect("inner must decode");

		assert_eq!(group, GroupId(77));
		assert_eq!(keyspace, Keyspace::EMIT);
		assert_eq!(suffix, vec![4, 5, 6]);
	}

	#[test]
	fn interned_group_keys_stay_compact() {
		// interning must keep state keys far smaller than embedding raw group bytes in every key
		let interned =
			OperatorStateKey::new(OperatorId(17), GroupId(123_456), Keyspace::ACCUMULATOR, vec![0; 8])
				.encode();
		let raw_group_bytes = b"7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU\
		                        So11111111111111111111111111111111111111112"
			.len() + 8;

		assert!(
			interned.as_slice().len() * 3 < raw_group_bytes,
			"an interned state key ({} bytes) must stay far below a raw-group key ({} bytes)",
			interned.as_slice().len(),
			raw_group_bytes
		);
	}

	#[test]
	fn the_ram_predicate_and_the_disk_range_agree_on_every_key() {
		// the RAM predicate and the disk range must agree on every key, or a phase-1 delete ghosts or strands a
		// row
		for group in GROUPS.map(GroupId) {
			let range = group_data_inner_range(group);
			for other in GROUPS.map(GroupId) {
				for keyspace in DATA_KEYSPACES.iter().chain(IDENTITY_KEYSPACES.iter()) {
					let key = OperatorStateKey::inner_encoded(other, *keyspace, vec![7, 7]);
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
		// the RAM predicate must never report an identity keyspace as reclaimable group data
		for keyspace in IDENTITY_KEYSPACES {
			let key = OperatorStateKey::inner_encoded(GroupId(9), keyspace, vec![1]);
			assert_eq!(
				group_data_of_inner(key.as_slice()),
				None,
				"{keyspace:?} must not be reported as reclaimable group data"
			);
		}
	}

	#[test]
	fn a_key_too_short_to_carry_a_keyspace_is_refused() {
		// a key without both a group and a keyspace byte must not decode as group data
		assert_eq!(group_data_of_inner(&[]), None);
		assert_eq!(group_data_of_inner(&[0xAB]), None, "a group with no keyspace byte must not decode");
	}

	#[test]
	fn the_predicate_agrees_with_the_disk_range_on_arbitrary_bytes() {
		// the predicate and the disk range must agree even on arbitrary bytes no encoder produced
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
	fn a_group_set_is_sorted_deduped_and_never_admits_root() {
		// a group set must stay sorted and deduped for binary_search, and must never admit the root group
		let set = GroupSet::new([GroupId(9), GroupId(2), GroupId(9), GroupId::ROOT, GroupId(5)]);

		assert_eq!(set.as_slice(), &[GroupId(2), GroupId(5), GroupId(9)]);
		assert_eq!(set.len(), 3);
		assert!(set.contains(GroupId(5)));
		assert!(!set.contains(GroupId(3)));
		assert!(!set.contains(GroupId::ROOT), "the root group must be filtered out, not merely unsorted");
	}

	#[test]
	fn an_empty_group_set_matches_nothing() {
		let set = GroupSet::new([]);

		assert!(set.is_empty());
		assert!(!set.contains(GroupId::FIRST));
	}

	#[test]
	fn a_group_set_hands_the_ffi_boundary_a_plain_u64_array() {
		// GroupId must stay repr(transparent) over u64, or the FFI slice cast reads the wrong bytes
		let set = GroupSet::new([GroupId(3), GroupId(1), GroupId(2)]);
		let (ptr, len) = set.as_raw_parts();

		assert_eq!(len, 3);
		// SAFETY: the slice is alive for the whole assertion and GroupId is repr(transparent) over u64.
		let raw = unsafe { slice::from_raw_parts(ptr, len) };
		assert_eq!(raw, &[1u64, 2, 3]);
	}
}
