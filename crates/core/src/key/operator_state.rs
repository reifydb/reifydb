// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Cow, ops::Bound};

use reifydb_codec::key::{
	deserializer::KeyDeserializer,
	encode_u8,
	encoded::{EncodedKey, EncodedKeyRange},
	serializer::KeySerializer,
};
use reifydb_value::util::hash::xxh3_128;

use super::{EncodableKey, KeyKind};
use crate::{
	interface::{catalog::flow::OperatorId, store::CacheTiers},
	metrics::heap::HeapSize,
};

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(pub u128);

impl GroupId {
	pub const ROOT: Self = Self(0);

	pub const FIRST_NON_ROOT: Self = Self(1);

	pub fn of(key: &EncodedKey) -> Self {
		let hash = xxh3_128(key.as_slice()).0;
		if hash == Self::ROOT.0 {
			Self::FIRST_NON_ROOT
		} else {
			Self(hash)
		}
	}

	pub fn is_root(&self) -> bool {
		*self == Self::ROOT
	}
}

impl HeapSize for GroupId {
	fn heap_size(&self) -> usize {
		0
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

	pub fn as_raw_parts(&self) -> (*const u128, usize) {
		(self.0.as_ptr() as *const u128, self.0.len())
	}
}

pub fn group_data_of_inner(inner: &[u8]) -> Option<GroupId> {
	let mut de = KeyDeserializer::from_bytes(inner);
	let group = GroupId(de.read_u128().ok()?);
	let keyspace = KeyspaceId(de.read_u8().ok()?);
	if !keyspace.is_data() {
		return None;
	}
	inner.starts_with(&group_inner_prefix(group)).then_some(group)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyspaceId(pub u8);

impl KeyspaceId {
	pub const HIGHEST_DATA: u8 = 0x7F;

	pub const ROW_NUMBER_MAPPING: Self = Self(0xFE);

	pub const NODE_COUNTER: Self = Self(0xFC);

	pub const SOURCE_WATERMARK: Self = Self(0xFA);

	pub const TIMER_WHEEL: Self = Self(0xF9);

	pub const TIMER_INDEX: Self = Self(0xF8);

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

	pub const JOIN_ROW_EXPIRY: Self = Self(0x2B);

	pub const CUSTOM_NOT_CACHED: Self = Self(0x40);

	pub fn name(&self) -> Cow<'static, str> {
		match *self {
			Self::ROW_NUMBER_MAPPING => "ROW_NUMBER_MAPPING",
			Self::NODE_COUNTER => "NODE_COUNTER",
			Self::SOURCE_WATERMARK => "SOURCE_WATERMARK",
			Self::TIMER_WHEEL => "TIMER_WHEEL",
			Self::TIMER_INDEX => "TIMER_INDEX",
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
			Self::JOIN_ROW_EXPIRY => "JOIN_ROW_EXPIRY",
			Self::CUSTOM_NOT_CACHED => "CUSTOM_NOT_CACHED",
			_ => return Cow::Owned(format!("{:#04x}", self.0)),
		}
		.into()
	}

	pub fn is_data(&self) -> bool {
		self.0 <= Self::HIGHEST_DATA
	}

	pub fn is_identity(&self) -> bool {
		!self.is_data()
	}

	pub fn cache_tiers(&self) -> CacheTiers {
		match *self {
			Self::CUSTOM_NOT_CACHED => CacheTiers::Neither,
			Self::EXPIRY => CacheTiers::Range,
			Self::TIMER_WHEEL => CacheTiers::Range,
			Self::ENGINE_META => CacheTiers::Range,
			Self::JOIN_PIN => CacheTiers::Range,
			Self::ROW_NUMBER_MAPPING => CacheTiers::Range,
			Self::ACCUMULATOR => CacheTiers::Range,
			Self::WINDOW_META => CacheTiers::Range,
			_ => CacheTiers::Both,
		}
	}

	pub fn is_guest_owned(&self) -> bool {
		matches!(*self, Self::CUSTOM_NOT_CACHED)
	}

	pub fn is_known(&self) -> bool {
		self.is_data()
			|| matches!(
				*self,
				Self::ROW_NUMBER_MAPPING
					| Self::NODE_COUNTER | Self::SOURCE_WATERMARK
					| Self::TIMER_WHEEL | Self::TIMER_INDEX
			)
	}
}

pub fn is_framed_inner(inner: &[u8]) -> bool {
	inner.is_empty() || OperatorStateKey::decode_inner(inner).is_some_and(|(_, keyspace, _)| keyspace.is_known())
}

pub fn is_guest_framed_inner(inner: &[u8]) -> bool {
	inner.is_empty()
		|| OperatorStateKey::decode_inner(inner).is_some_and(|(_, keyspace, _)| keyspace.is_guest_owned())
}

pub fn is_identity_framed_inner(inner: &[u8]) -> bool {
	OperatorStateKey::decode_inner(inner)
		.is_some_and(|(_, keyspace, _)| keyspace.is_identity() && keyspace.is_known())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorStateKey {
	pub operator: OperatorId,
	pub group: GroupId,
	pub keyspace: KeyspaceId,
	pub suffix: Vec<u8>,
}

impl OperatorStateKey {
	pub fn new(operator: OperatorId, group: GroupId, keyspace: KeyspaceId, suffix: impl Into<Vec<u8>>) -> Self {
		Self {
			operator,
			group,
			keyspace,
			suffix: suffix.into(),
		}
	}

	pub fn root(operator: OperatorId, keyspace: KeyspaceId, suffix: impl Into<Vec<u8>>) -> Self {
		Self::new(operator, GroupId::ROOT, keyspace, suffix)
	}

	pub fn encoded(
		operator: OperatorId,
		group: GroupId,
		keyspace: KeyspaceId,
		suffix: impl AsRef<[u8]>,
	) -> EncodedKey {
		let suffix = suffix.as_ref();
		let mut serializer = KeySerializer::with_capacity(28 + suffix.len());
		serializer
			.extend_u8(KeyKind::OperatorState as u8)
			.extend_u64(operator.0)
			.extend_u128(group.0)
			.extend_u8(keyspace.0)
			.extend_raw(suffix);
		serializer.to_encoded_key()
	}

	pub fn inner(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(20 + self.suffix.len());
		serializer.extend_u128(self.group.0).extend_u8(self.keyspace.0).extend_raw(&self.suffix);
		serializer.to_encoded_key()
	}

	pub const KEYSPACE_INNER_OFFSET: u32 = size_of::<u128>() as u32;

	pub fn decode_keyspace(stored: u8) -> KeyspaceId {
		KeyspaceId(KeyDeserializer::from_bytes(&[stored]).read_u8().expect("a single byte decodes as u8"))
	}

	pub fn inner_encoded(group: GroupId, keyspace: KeyspaceId, suffix: impl AsRef<[u8]>) -> GroupStateKey {
		let suffix = suffix.as_ref();
		let mut serializer = KeySerializer::with_capacity(20 + suffix.len());
		serializer.extend_u128(group.0).extend_u8(keyspace.0).extend_raw(suffix);
		GroupStateKey(serializer.to_encoded_key())
	}

	pub fn decode_inner(inner: &[u8]) -> Option<(GroupId, KeyspaceId, Vec<u8>)> {
		let mut de = KeyDeserializer::from_bytes(inner);
		let group = de.read_u128().ok()?;
		let keyspace = de.read_u8().ok()?;
		let suffix = de.read_raw(de.remaining()).ok()?.to_vec();
		Some((GroupId(group), KeyspaceId(keyspace), suffix))
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
		let mut serializer = KeySerializer::with_capacity(28 + self.suffix.len());
		serializer
			.extend_u8(KeyKind::OperatorState as u8)
			.extend_u64(self.operator.0)
			.extend_u128(self.group.0)
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
		let group = de.read_u128().ok()?;
		let keyspace = de.read_u8().ok()?;
		let suffix = de.read_raw(de.remaining()).ok()?.to_vec();

		Some(Self {
			operator: OperatorId(operator),
			group: GroupId(group),
			keyspace: KeyspaceId(keyspace),
			suffix,
		})
	}
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupStateKey(EncodedKey);

impl GroupStateKey {
	pub fn new(group: GroupId, keyspace: KeyspaceId, suffix: impl AsRef<[u8]>) -> Self {
		OperatorStateKey::inner_encoded(group, keyspace, suffix)
	}

	pub fn root(keyspace: KeyspaceId, suffix: impl AsRef<[u8]>) -> Self {
		Self::new(GroupId::ROOT, keyspace, suffix)
	}

	pub fn from_framed(key: EncodedKey) -> Option<Self> {
		is_framed_inner(key.as_slice()).then_some(Self(key))
	}

	pub fn from_guest_framed(key: EncodedKey) -> Option<Self> {
		is_guest_framed_inner(key.as_slice()).then_some(Self(key))
	}

	pub fn from_identity_framed(key: EncodedKey) -> Option<Self> {
		is_identity_framed_inner(key.as_slice()).then_some(Self(key))
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

	pub fn keyspace(&self) -> Option<KeyspaceId> {
		let bytes = self.0.as_slice();
		let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
		(bytes.len() > offset).then(|| KeyspaceId(encode_u8(bytes[offset])))
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
	let mut serializer = KeySerializer::with_capacity(20);
	serializer.extend_u128(group.0);
	serializer.finish().as_ref().to_vec()
}

fn keyspace_inner_prefix(group: GroupId, keyspace: KeyspaceId) -> Vec<u8> {
	let mut prefix = group_inner_prefix(group);
	prefix.push(encode_u8(keyspace.0));
	prefix
}

pub fn group_inner_range(group: GroupId) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&group_inner_prefix(group))
}

pub fn keyspace_inner_range(group: GroupId, keyspace: KeyspaceId) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&keyspace_inner_prefix(group, keyspace))
}

pub const ROW_NUMBER_COUNTER_SUFFIX: &[u8] = b"rn";

pub fn row_number_counter_key() -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::NODE_COUNTER, ROW_NUMBER_COUNTER_SUFFIX)
}

pub fn keyspace_inner_range_upto(group: GroupId, keyspace: KeyspaceId, suffix: &[u8]) -> EncodedKeyRange {
	let mut bound = keyspace_inner_prefix(group, keyspace);
	bound.extend_from_slice(suffix);
	EncodedKeyRange::new(keyspace_inner_range(group, keyspace).start, EncodedKeyRange::prefix(&bound).end)
}

pub fn group_data_inner_range(group: GroupId) -> EncodedKeyRange {
	let prefix = group_inner_prefix(group);
	let mut start = prefix.clone();
	start.push(encode_u8(KeyspaceId::HIGHEST_DATA));
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(start)), EncodedKeyRange::prefix(&prefix).end)
}

pub fn group_identity_inner_range(group: GroupId) -> EncodedKeyRange {
	let prefix = group_inner_prefix(group);
	let mut end = prefix.clone();
	end.push(encode_u8(KeyspaceId::HIGHEST_DATA));
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(prefix)), Bound::Excluded(EncodedKey::new(end)))
}

pub const NODE_PREFIX_LEN: usize = 9;

pub fn extend_node_prefix(serializer: &mut KeySerializer, operator: OperatorId) {
	serializer.extend_u8(KeyKind::OperatorState as u8).extend_u64(operator.0);
}

pub fn node_prefix(operator: OperatorId) -> Vec<u8> {
	let mut serializer = KeySerializer::with_capacity(NODE_PREFIX_LEN);
	extend_node_prefix(&mut serializer, operator);
	serializer.finish().as_ref().to_vec()
}

fn group_prefix(operator: OperatorId, group: GroupId) -> Vec<u8> {
	let mut serializer = KeySerializer::with_capacity(28);
	serializer.extend_u8(KeyKind::OperatorState as u8).extend_u64(operator.0).extend_u128(group.0);
	serializer.finish().as_ref().to_vec()
}

fn keyspace_prefix(operator: OperatorId, group: GroupId, keyspace: KeyspaceId) -> Vec<u8> {
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

pub fn keyspace_range(operator: OperatorId, group: GroupId, keyspace: KeyspaceId) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&keyspace_prefix(operator, group, keyspace))
}

pub fn group_data_range(operator: OperatorId, group: GroupId) -> EncodedKeyRange {
	let prefix = group_prefix(operator, group);
	let mut start = prefix.clone();
	start.push(encode_u8(KeyspaceId::HIGHEST_DATA));
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(start)), EncodedKeyRange::prefix(&prefix).end)
}

pub fn group_identity_range(operator: OperatorId, group: GroupId) -> EncodedKeyRange {
	let prefix = group_prefix(operator, group);
	let mut end = prefix.clone();
	end.push(encode_u8(KeyspaceId::HIGHEST_DATA));
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(prefix)), Bound::Excluded(EncodedKey::new(end)))
}

#[cfg(test)]
mod tests {
	use std::{ops::Bound, slice};

	use super::{
		CacheTiers, EncodedKey, EncodedKeyRange, GroupId, GroupSet, KeySerializer, KeyspaceId,
		OperatorStateKey, group_data_inner_range, group_data_of_inner, group_data_range,
		group_identity_inner_range, group_identity_range, group_inner_prefix, group_inner_range, group_range,
		is_framed_inner, keyspace_range, node_prefix, node_range,
	};
	use crate::{interface::catalog::flow::OperatorId, key::EncodableKey};

	const NODES: [u64; 4] = [1, 17, 300, 70_000];
	const GROUPS: [u128; 8] = [1, 2, 127, 128, 1000, 100_000, 1 << 30, u128::MAX];
	const DATA_KEYSPACES: [KeyspaceId; 4] =
		[KeyspaceId::ACCUMULATOR, KeyspaceId::BUFFER, KeyspaceId::RUNNING, KeyspaceId::CUSTOM_NOT_CACHED];
	const IDENTITY_KEYSPACES: [KeyspaceId; 1] = [KeyspaceId::ROW_NUMBER_MAPPING];

	#[derive(Clone, Copy, PartialEq, Debug)]
	enum Phase {
		Data,
		Identity,
	}

	/// Every keyspace the substrate declares, with the phase allowed to erase it and the tiers it may
	/// be cached in. Both are written down rather than read back from `is_data` and `cache_tiers`, or
	/// a keyspace changing sides would pass unremarked.
	const CENSUS: [(&str, KeyspaceId, Phase, CacheTiers); 33] = [
		("ROW_NUMBER_MAPPING", KeyspaceId::ROW_NUMBER_MAPPING, Phase::Identity, CacheTiers::Range),
		("NODE_COUNTER", KeyspaceId::NODE_COUNTER, Phase::Identity, CacheTiers::Both),
		("SOURCE_WATERMARK", KeyspaceId::SOURCE_WATERMARK, Phase::Identity, CacheTiers::Both),
		("TIMER_WHEEL", KeyspaceId::TIMER_WHEEL, Phase::Identity, CacheTiers::Range),
		("TIMER_INDEX", KeyspaceId::TIMER_INDEX, Phase::Identity, CacheTiers::Both),
		("ACCUMULATOR", KeyspaceId::ACCUMULATOR, Phase::Data, CacheTiers::Range),
		("BUFFER", KeyspaceId::BUFFER, Phase::Data, CacheTiers::Both),
		("RUNNING", KeyspaceId::RUNNING, Phase::Data, CacheTiers::Both),
		("EMIT", KeyspaceId::EMIT, Phase::Data, CacheTiers::Both),
		("EXPIRY", KeyspaceId::EXPIRY, Phase::Data, CacheTiers::Range),
		("COUNT", KeyspaceId::COUNT, Phase::Data, CacheTiers::Both),
		("ROW_INDEX", KeyspaceId::ROW_INDEX, Phase::Data, CacheTiers::Both),
		("SESSION", KeyspaceId::SESSION, Phase::Data, CacheTiers::Both),
		("ROLLING_META", KeyspaceId::ROLLING_META, Phase::Data, CacheTiers::Both),
		("ENGINE_META", KeyspaceId::ENGINE_META, Phase::Data, CacheTiers::Range),
		("DISTINCT_ENTRY", KeyspaceId::DISTINCT_ENTRY, Phase::Data, CacheTiers::Both),
		("WINDOW_META", KeyspaceId::WINDOW_META, Phase::Data, CacheTiers::Range),
		("JOIN_LEFT", KeyspaceId::JOIN_LEFT, Phase::Data, CacheTiers::Both),
		("JOIN_RIGHT", KeyspaceId::JOIN_RIGHT, Phase::Data, CacheTiers::Both),
		("JOIN_SCHEMA", KeyspaceId::JOIN_SCHEMA, Phase::Data, CacheTiers::Both),
		("RINGBUFFER_FORWARD", KeyspaceId::RINGBUFFER_FORWARD, Phase::Data, CacheTiers::Both),
		("RINGBUFFER_ENTRY", KeyspaceId::RINGBUFFER_ENTRY, Phase::Data, CacheTiers::Both),
		("GATE_VISIBILITY", KeyspaceId::GATE_VISIBILITY, Phase::Data, CacheTiers::Both),
		("DISTINCT_LAYOUT", KeyspaceId::DISTINCT_LAYOUT, Phase::Data, CacheTiers::Both),
		("RINGBUFFER_EXPIRY", KeyspaceId::RINGBUFFER_EXPIRY, Phase::Data, CacheTiers::Both),
		("RINGBUFFER_TTL_ARM", KeyspaceId::RINGBUFFER_TTL_ARM, Phase::Data, CacheTiers::Both),
		("SEAL_LEDGER", KeyspaceId::SEAL_LEDGER, Phase::Data, CacheTiers::Both),
		("JOIN_PUBLISHED", KeyspaceId::JOIN_PUBLISHED, Phase::Data, CacheTiers::Both),
		("JOIN_PIN", KeyspaceId::JOIN_PIN, Phase::Data, CacheTiers::Range),
		("RINGBUFFER_META", KeyspaceId::RINGBUFFER_META, Phase::Data, CacheTiers::Both),
		("REAP_QUEUE", KeyspaceId::REAP_QUEUE, Phase::Data, CacheTiers::Both),
		("JOIN_ROW_EXPIRY", KeyspaceId::JOIN_ROW_EXPIRY, Phase::Data, CacheTiers::Both),
		("CUSTOM_NOT_CACHED", KeyspaceId::CUSTOM_NOT_CACHED, Phase::Data, CacheTiers::Neither),
	];

	/// Counts `KeyspaceId` constants from the source text. There is no reflection over associated
	/// constants, so this is the only way the census can notice a keyspace nobody listed.
	fn declared_keyspaces() -> usize {
		let source = include_str!("operator_state.rs");
		let body = source
			.split("impl KeyspaceId {")
			.nth(1)
			.expect("the KeyspaceId impl block is where the constants are declared");
		let body = body.split("\n}\n").next().expect("the impl block is closed");
		body.lines()
			.filter(|line| {
				let line = line.trim_start();
				line.starts_with("pub const") && line.contains("Self(")
			})
			.count()
	}

	#[test]
	fn a_bare_row_number_key_is_too_short_to_be_read_as_a_framed_key() {
		// a bare u64 is half a group prefix, so the framing check must decline it rather than read past its end
		let mut bare = KeySerializer::with_capacity(4);
		bare.extend_u64(7u64);
		let bare = bare.finish().as_ref().to_vec();

		assert!(bare.len() < group_inner_prefix(GroupId(7)).len(), "a bare row number cannot span a group");
		assert!(OperatorStateKey::decode_inner(&bare).is_none());
		assert!(!is_framed_inner(&bare));

		let framed = OperatorStateKey::inner_encoded(
			GroupId::ROOT,
			KeyspaceId::CUSTOM_NOT_CACHED,
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
				KeyspaceId::NODE_COUNTER,
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
				OperatorStateKey::new(OperatorId(1), GroupId(*group), KeyspaceId::ACCUMULATOR, vec![])
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
		for (name, keyspace, _, _) in CENSUS {
			assert_eq!(
				keyspace.name(),
				name,
				"{name} ({:#04x}) does not name itself, so an offline census reports it as CUSTOM",
				keyspace.0
			);
		}

		assert_eq!(
			KeyspaceId::CUSTOM_NOT_CACHED.name(),
			"CUSTOM_NOT_CACHED",
			"a custom keyspace names the admission side it sits on; there is no unnamed fallback to absorb it"
		);
	}

	#[test]
	fn every_declared_keyspace_states_the_tiers_it_may_be_cached_in() {
		// The census names the policy so a keyspace moving between tiers has to be moved here too. A
		// wrong side is silent: the tier just declines every span and the keyspace reads sqlite forever,
		// which reads as a cold cache rather than as a policy mistake.
		for (name, keyspace, _, policy) in CENSUS {
			assert_eq!(
				keyspace.cache_tiers(),
				policy,
				"{name} ({:#04x}) is cached on a different side than the census records",
				keyspace.0
			);
		}

		let neither: Vec<&str> =
			CENSUS.iter().filter(|(_, _, _, p)| *p == CacheTiers::Neither).map(|(n, ..)| *n).collect();
		assert_eq!(
			neither,
			["CUSTOM_NOT_CACHED"],
			"widening the set a tier refuses turns that tier into an off switch and only shows up as a \
			 throughput loss in a replay, so every move in or out is a measured decision"
		);

		assert!(
			KeyspaceId(0x43).cache_tiers() == CacheTiers::Both,
			"an undeclared keyspace must default to cacheable, or a custom operator silently loses \
			 both tiers"
		);
	}

	#[test]
	fn every_declared_keyspace_is_distinct_framing_and_swept_by_exactly_one_phase() {
		// every declared keyspace must have a unique byte and belong to exactly one reclamation phase
		assert_eq!(
			CENSUS.len(),
			declared_keyspaces(),
			"a keyspace was added to KeyspaceId without being added to the census, so nothing below \
			 ever looks at its byte"
		);

		let mut seen: Vec<(&str, u8)> = Vec::new();
		for (name, keyspace, phase, _) in CENSUS {
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
		// the root-scoped counter must sit outside every group range, or reclaiming a group erases it
		for operator in NODES {
			let counter = OperatorStateKey::root(
				OperatorId(operator),
				KeyspaceId::NODE_COUNTER,
				b"mint".to_vec(),
			)
			.encode();
			for group in GROUPS {
				let range = group_range(OperatorId(operator), GroupId(group));
				assert!(
					!contains(&range, counter.as_slice()),
					"group {group} range must not contain the root group's counter"
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
		let range = keyspace_range(operator, group, KeyspaceId::BUFFER);

		let inside = OperatorStateKey::new(operator, group, KeyspaceId::BUFFER, vec![1]).encode();
		assert!(contains(&range, inside.as_slice()));

		for other in [KeyspaceId::ACCUMULATOR, KeyspaceId::RUNNING, KeyspaceId::ROW_NUMBER_MAPPING] {
			let key = OperatorStateKey::new(operator, group, other, vec![1]).encode();
			assert!(!contains(&range, key.as_slice()), "keyspace {other:?} leaked into the buffer range");
		}

		let other_group = OperatorStateKey::new(operator, GroupId(43), KeyspaceId::BUFFER, vec![1]).encode();
		assert!(!contains(&range, other_group.as_slice()), "another group's buffer leaked into the range");
	}

	#[test]
	fn encode_decode_round_trips_every_component() {
		let key = OperatorStateKey::new(
			OperatorId(0xDEAD_BEEF),
			GroupId(123_456),
			KeyspaceId::CUSTOM_NOT_CACHED,
			vec![1, 2, 3, 4],
		);
		assert_eq!(OperatorStateKey::decode(&key.encode()), Some(key));
	}

	#[test]
	fn keys_still_decode_as_operator_state_of_their_node() {
		// every key of this kind must still decode to its own operator, or state is misrouted into the CDC log
		let key = OperatorStateKey::new(OperatorId(9), GroupId(4), KeyspaceId::ACCUMULATOR, vec![1]).encode();

		let decoded = OperatorStateKey::decode(&key).expect("must remain decodable as its key kind");
		assert_eq!(decoded.operator, OperatorId(9));
	}

	#[test]
	fn an_inner_key_composed_with_its_node_prefix_reproduces_the_full_key() {
		// inner key plus node prefix must reproduce the full key, or state written through the API is
		// unreachable
		let key = OperatorStateKey::new(OperatorId(17), GroupId(42), KeyspaceId::BUFFER, vec![9, 9]);

		let mut composed = node_prefix(OperatorId(17));
		composed.extend_from_slice(key.inner().as_slice());

		assert_eq!(composed, key.encode().as_slice(), "inner key plus operator prefix must equal the full key");
	}

	#[test]
	fn the_root_group_range_stays_inside_its_node() {
		// group 0's inner range has no byte-wise successor, so it must stay bounded by the operator prefix
		let range = group_inner_range(GroupId::ROOT).with_prefix(EncodedKey::new(node_prefix(OperatorId(17))));

		let own = OperatorStateKey::root(OperatorId(17), KeyspaceId::NODE_COUNTER, vec![1]).encode();
		assert!(contains(&range, own.as_slice()), "the operator's own counter must be in range");

		for operator in NODES {
			if operator == 17 {
				continue;
			}
			for keyspace in [KeyspaceId::NODE_COUNTER, KeyspaceId::ACCUMULATOR] {
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
		let key = OperatorStateKey::new(OperatorId(3), GroupId(77), KeyspaceId::EMIT, vec![4, 5, 6]);
		let (group, keyspace, suffix) =
			OperatorStateKey::decode_inner(key.inner().as_slice()).expect("inner must decode");

		assert_eq!(group, GroupId(77));
		assert_eq!(keyspace, KeyspaceId::EMIT);
		assert_eq!(suffix, vec![4, 5, 6]);
	}

	#[test]
	fn a_state_key_stays_compact_however_long_the_group_key_is() {
		// a hashed group id must keep the state key fixed-width, never embedding the raw group bytes
		let long = b"7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU\
		             So11111111111111111111111111111111111111112";
		let hashed = OperatorStateKey::new(
			OperatorId(17),
			GroupId::of(&EncodedKey::new(long.to_vec())),
			KeyspaceId::ACCUMULATOR,
			vec![0; 8],
		)
		.encode();
		let short = OperatorStateKey::new(
			OperatorId(17),
			GroupId::of(&EncodedKey::new(b"g".to_vec())),
			KeyspaceId::ACCUMULATOR,
			vec![0; 8],
		)
		.encode();

		assert_eq!(hashed.as_slice().len(), short.as_slice().len(), "group key length must not reach the key");
		assert!(hashed.as_slice().len() * 2 < long.len() + 8, "and must stay far below embedding the bytes");
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
		assert!(!set.contains(GroupId::FIRST_NON_ROOT));
	}

	#[test]
	fn a_group_set_hands_the_ffi_boundary_a_plain_u128_array() {
		// GroupId must stay repr(transparent) over u128, or the FFI slice cast reads the wrong bytes
		let set = GroupSet::new([GroupId(3), GroupId(1), GroupId(2)]);
		let (ptr, len) = set.as_raw_parts();

		assert_eq!(len, 3);
		// SAFETY: the slice is alive for the whole assertion and GroupId is repr(transparent) over u128.
		let raw = unsafe { slice::from_raw_parts(ptr, len) };
		assert_eq!(raw, &[1u128, 2, 3]);
	}
}
