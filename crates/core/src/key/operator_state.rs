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
use crate::interface::catalog::flow::FlowNodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(pub u64);

impl GroupId {
	pub const NODE_SCOPE: Self = Self(0);

	pub const FIRST: Self = Self(1);

	pub fn is_node_scope(&self) -> bool {
		*self == Self::NODE_SCOPE
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Keyspace(pub u8);

impl Keyspace {
	pub const HIGHEST_DATA: u8 = 0x7F;

	pub const GROUP_META: Self = Self(0xFF);

	pub const ROW_NUMBER_MAPPING: Self = Self(0xFE);

	pub const GROUP_DICTIONARY: Self = Self(0xFD);

	pub const NODE_COUNTER: Self = Self(0xFC);

	pub const ACCUMULATOR: Self = Self(0x10);

	pub const BUFFER: Self = Self(0x11);

	pub const RUNNING: Self = Self(0x12);

	pub const EMIT: Self = Self(0x13);

	pub const EXPIRY: Self = Self(0x14);

	pub const WATERMARK: Self = Self(0x15);

	pub const COUNT: Self = Self(0x16);

	pub const ROW_INDEX: Self = Self(0x17);

	pub const SESSION: Self = Self(0x18);

	pub const ROLLING_META: Self = Self(0x19);

	pub const ENGINE_META: Self = Self(0x1A);

	pub const GATE_VISIBILITY: Self = Self(0x1B);

	pub const DISTINCT_SLOT: Self = Self(0x1C);

	pub const DISTINCT_ENTRY: Self = Self(0x1D);

	pub const APPEND_TIMESTAMP: Self = Self(0x1E);

	pub const FIRST_CUSTOM: Self = Self(0x40);

	pub fn is_data(&self) -> bool {
		self.0 <= Self::HIGHEST_DATA
	}

	pub fn is_identity(&self) -> bool {
		!self.is_data()
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorStateKey {
	pub node: FlowNodeId,
	pub group: GroupId,
	pub keyspace: Keyspace,
	pub suffix: Vec<u8>,
}

impl OperatorStateKey {
	pub fn new(node: FlowNodeId, group: GroupId, keyspace: Keyspace, suffix: impl Into<Vec<u8>>) -> Self {
		Self {
			node,
			group,
			keyspace,
			suffix: suffix.into(),
		}
	}

	pub fn node_scoped(node: FlowNodeId, keyspace: Keyspace, suffix: impl Into<Vec<u8>>) -> Self {
		Self::new(node, GroupId::NODE_SCOPE, keyspace, suffix)
	}

	pub fn encode(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(20 + self.suffix.len());
		serializer
			.extend_u8(KeyKind::FlowNodeInternalState as u8)
			.extend_u64(self.node.0)
			.extend_u64(self.group.0)
			.extend_u8(self.keyspace.0)
			.extend_raw(&self.suffix);
		serializer.to_encoded_key()
	}

	pub fn decode(key: &EncodedKey) -> Option<Self> {
		let mut de = KeyDeserializer::from_bytes(key.as_slice());

		let kind: KeyKind = de.read_u8().ok()?.try_into().ok()?;
		if kind != KeyKind::FlowNodeInternalState {
			return None;
		}

		let node = de.read_u64().ok()?;
		let group = de.read_u64().ok()?;
		let keyspace = de.read_u8().ok()?;
		let suffix = de.read_raw(de.remaining()).ok()?.to_vec();

		Some(Self {
			node: FlowNodeId(node),
			group: GroupId(group),
			keyspace: Keyspace(keyspace),
			suffix,
		})
	}

	pub fn encoded(
		node: FlowNodeId,
		group: GroupId,
		keyspace: Keyspace,
		suffix: impl Into<Vec<u8>>,
	) -> EncodedKey {
		Self::new(node, group, keyspace, suffix).encode()
	}

	pub fn inner(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(12 + self.suffix.len());
		serializer.extend_u64(self.group.0).extend_u8(self.keyspace.0).extend_raw(&self.suffix);
		serializer.to_encoded_key()
	}

	pub fn inner_encoded(group: GroupId, keyspace: Keyspace, suffix: impl Into<Vec<u8>>) -> EncodedKey {
		Self::new(FlowNodeId(0), group, keyspace, suffix).inner()
	}

	pub fn decode_inner(inner: &[u8]) -> Option<(GroupId, Keyspace, Vec<u8>)> {
		let mut de = KeyDeserializer::from_bytes(inner);
		let group = de.read_u64().ok()?;
		let keyspace = de.read_u8().ok()?;
		let suffix = de.read_raw(de.remaining()).ok()?.to_vec();
		Some((GroupId(group), Keyspace(keyspace), suffix))
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

fn node_prefix(node: FlowNodeId) -> Vec<u8> {
	let mut serializer = KeySerializer::with_capacity(12);
	serializer.extend_u8(KeyKind::FlowNodeInternalState as u8).extend_u64(node.0);
	serializer.finish().as_ref().to_vec()
}

fn group_prefix(node: FlowNodeId, group: GroupId) -> Vec<u8> {
	let mut serializer = KeySerializer::with_capacity(20);
	serializer.extend_u8(KeyKind::FlowNodeInternalState as u8).extend_u64(node.0).extend_u64(group.0);
	serializer.finish().as_ref().to_vec()
}

fn keyspace_prefix(node: FlowNodeId, group: GroupId, keyspace: Keyspace) -> Vec<u8> {
	let mut prefix = group_prefix(node, group);
	prefix.push(encode_u8(keyspace.0));
	prefix
}

pub fn node_range(node: FlowNodeId) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&node_prefix(node))
}

pub fn group_range(node: FlowNodeId, group: GroupId) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&group_prefix(node, group))
}

pub fn keyspace_range(node: FlowNodeId, group: GroupId, keyspace: Keyspace) -> EncodedKeyRange {
	EncodedKeyRange::prefix(&keyspace_prefix(node, group, keyspace))
}

pub fn group_data_range(node: FlowNodeId, group: GroupId) -> EncodedKeyRange {
	let prefix = group_prefix(node, group);
	let mut start = prefix.clone();
	start.push(encode_u8(Keyspace::HIGHEST_DATA));
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(start)), EncodedKeyRange::prefix(&prefix).end)
}

pub fn group_identity_range(node: FlowNodeId, group: GroupId) -> EncodedKeyRange {
	let prefix = group_prefix(node, group);
	let mut end = prefix.clone();
	end.push(encode_u8(Keyspace::HIGHEST_DATA));
	EncodedKeyRange::new(Bound::Included(EncodedKey::new(prefix)), Bound::Excluded(EncodedKey::new(end)))
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use super::{
		GroupId, Keyspace, OperatorStateKey, group_data_inner_range, group_data_range,
		group_identity_inner_range, group_identity_range, group_inner_range, group_range, keyspace_range,
		node_range,
	};
	use crate::{
		interface::{
			catalog::flow::FlowNodeId,
			store::{EntryKind, classify_key},
		},
		key::{EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey},
	};

	const NODES: [u64; 4] = [1, 17, 300, 70_000];
	const GROUPS: [u64; 8] = [1, 2, 127, 128, 1000, 100_000, 1 << 30, u64::MAX];
	const DATA_KEYSPACES: [Keyspace; 4] =
		[Keyspace::ACCUMULATOR, Keyspace::BUFFER, Keyspace::RUNNING, Keyspace::FIRST_CUSTOM];
	const IDENTITY_KEYSPACES: [Keyspace; 2] = [Keyspace::GROUP_META, Keyspace::ROW_NUMBER_MAPPING];

	fn contains(range: &reifydb_codec::key::encoded::EncodedKeyRange, key: &[u8]) -> bool {
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
		for node in NODES {
			for group in GROUPS {
				for keyspace in DATA_KEYSPACES.iter().chain(IDENTITY_KEYSPACES.iter()) {
					for coord in [0u64, 1, 999, u64::MAX] {
						keys.push(OperatorStateKey::new(
							FlowNodeId(node),
							GroupId(group),
							*keyspace,
							coord.to_be_bytes().to_vec(),
						));
					}
				}
			}
			keys.push(OperatorStateKey::node_scoped(
				FlowNodeId(node),
				Keyspace::GROUP_DICTIONARY,
				b"7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU".to_vec(),
			));
		}
		keys
	}

	#[test]
	fn a_group_range_contains_exactly_that_groups_keys() {
		// The whole reclamation design rests on this: erasing a group is a single bounded range
		// delete, and completeness is structural rather than a registry of keyspaces. If a range
		// could swallow a neighbouring group's keys this silently destroys live state; if it could
		// miss any of its own, the leak this step exists to close survives.
		let population = population();
		for node in NODES {
			for group in GROUPS {
				let range = group_range(FlowNodeId(node), GroupId(group));
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
		// Group ids are varint encoded, so ids of different magnitudes produce different byte
		// lengths. If a short id's encoding were a prefix of a longer one's, group 1's range would
		// contain group 1000's keys and reclaiming the former would erase the latter. This is the
		// property that makes the range test above hold for every id, not just the ones sampled.
		let encodings: Vec<Vec<u8>> = GROUPS
			.iter()
			.map(|group| {
				OperatorStateKey::new(FlowNodeId(1), GroupId(*group), Keyspace::ACCUMULATOR, vec![])
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
		// Two-phase reclamation is only one range operation per phase because the keyspace byte
		// orders identity before data after inversion. If the split leaked either way, phase 1
		// would either take the row-number mapping with it (duplicate rows on the next wake,
		// landmine L2) or leave live accumulators behind (the leak survives).
		for node in NODES {
			for group in GROUPS {
				let data = group_data_range(FlowNodeId(node), GroupId(group));
				let identity = group_identity_range(FlowNodeId(node), GroupId(group));

				for keyspace in DATA_KEYSPACES {
					let key = OperatorStateKey::new(
						FlowNodeId(node),
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
						FlowNodeId(node),
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
	fn node_scoped_entries_sit_outside_every_group_range() {
		// The interning dictionary and the due-ordered expiry index live at node scope. Reclaiming a
		// group must not touch the table that resolves group ids, or the substrate would erase its
		// own address book while other groups still depend on it.
		for node in NODES {
			let dictionary = OperatorStateKey::node_scoped(
				FlowNodeId(node),
				Keyspace::GROUP_DICTIONARY,
				b"mint-pubkey".to_vec(),
			)
			.encode();
			for group in GROUPS {
				let range = group_range(FlowNodeId(node), GroupId(group));
				assert!(
					!contains(&range, dictionary.as_slice()),
					"group {group} range must not contain the node-scope dictionary"
				);
			}
		}
	}

	#[test]
	fn a_node_range_contains_exactly_that_nodes_keys() {
		// drop_flow_node erases a whole node by range. Containing a neighbour's keys would destroy a
		// live flow's state; missing its own would strand the keyspace exactly as it does today.
		let population = population();
		for node in NODES {
			let range = node_range(FlowNodeId(node));
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
		// Hydration and per-keyspace scans read through this range. Bleeding into an adjacent
		// keyspace would feed one cache another's payloads, which fails to decode at best and
		// silently mixes state at worst.
		let node = FlowNodeId(17);
		let group = GroupId(42);
		let range = keyspace_range(node, group, Keyspace::BUFFER);

		let inside = OperatorStateKey::new(node, group, Keyspace::BUFFER, vec![1]).encode();
		assert!(contains(&range, inside.as_slice()));

		for other in [Keyspace::ACCUMULATOR, Keyspace::RUNNING, Keyspace::GROUP_META] {
			let key = OperatorStateKey::new(node, group, other, vec![1]).encode();
			assert!(!contains(&range, key.as_slice()), "keyspace {other:?} leaked into the buffer range");
		}

		let other_group = OperatorStateKey::new(node, GroupId(43), Keyspace::BUFFER, vec![1]).encode();
		assert!(!contains(&range, other_group.as_slice()), "another group's buffer leaked into the range");
	}

	#[test]
	fn encode_decode_round_trips_every_component() {
		let key = OperatorStateKey::new(
			FlowNodeId(0xDEAD_BEEF),
			GroupId(123_456),
			Keyspace::FIRST_CUSTOM,
			vec![1, 2, 3, 4],
		);
		assert_eq!(OperatorStateKey::decode(&key.encode()), Some(key));
	}

	#[test]
	fn keys_still_classify_as_operator_internal_state_of_their_node() {
		// The codec deliberately keeps KeyKind::FlowNodeInternalState so tier classification and the
		// compiler-forced CDC exclusion keep working untouched. If a structured key stopped
		// classifying as OperatorInternal it would be routed to the wrong tier and start appearing
		// in the CDC log, which operator state must never do.
		let key = OperatorStateKey::new(FlowNodeId(9), GroupId(4), Keyspace::ACCUMULATOR, vec![1]).encode();

		assert_eq!(classify_key(&key), EntryKind::OperatorInternal(FlowNodeId(9)));

		let legacy = FlowNodeInternalStateKey::decode(&key).expect("must remain decodable as its key kind");
		assert_eq!(legacy.node, FlowNodeId(9));
	}

	#[test]
	fn an_inner_key_composed_with_its_node_prefix_reproduces_the_full_key() {
		// The state API owns the [kind][node] head and callers supply only the tail, so the two forms
		// must agree exactly. If they drifted, a key written through the state API would be
		// unreachable by a range built from the full-key helpers - state would be silently stranded
		// where reclamation cannot see it.
		let key = OperatorStateKey::new(FlowNodeId(17), GroupId(42), Keyspace::BUFFER, vec![9, 9]);

		let mut composed = FlowNodeInternalStateKey::encoded(FlowNodeId(17), vec![]).as_slice().to_vec();
		composed.extend_from_slice(key.inner().as_slice());

		assert_eq!(composed, key.encode().as_slice(), "inner key plus node prefix must equal the full key");
	}

	#[test]
	fn the_node_scope_group_range_stays_inside_its_node() {
		// Group 0 encodes as 0xFF, so its inner prefix is all-ones and has no byte-wise successor:
		// EncodedKeyRange::prefix yields an unbounded end. Composed with the node prefix that
		// degrades to "the rest of this node", which is exactly group 0's keys because it sorts last
		// within the node. Were it to stay unbounded, hydrating the interning dictionary would walk
		// into the next node's state.
		let range = group_inner_range(GroupId::NODE_SCOPE)
			.with_prefix(FlowNodeInternalStateKey::encoded(FlowNodeId(17), vec![]));

		let own = OperatorStateKey::node_scoped(FlowNodeId(17), Keyspace::GROUP_DICTIONARY, vec![1]).encode();
		assert!(contains(&range, own.as_slice()), "the node's own dictionary entry must be in range");

		for node in NODES {
			if node == 17 {
				continue;
			}
			for keyspace in [Keyspace::GROUP_DICTIONARY, Keyspace::ACCUMULATOR] {
				let foreign = OperatorStateKey::new(
					FlowNodeId(node),
					GroupId::NODE_SCOPE,
					keyspace,
					vec![1],
				)
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
		// Reclamation runs through the state API, so the inner forms are the ones that actually
		// execute. A split that held only for full keys would pass the phase test above and still
		// take the row-number mapping with phase 1.
		let node = FlowNodeId(17);
		let prefix = FlowNodeInternalStateKey::encoded(node, vec![]);
		for group in GROUPS {
			let data = group_data_inner_range(GroupId(group)).with_prefix(prefix.clone());
			let identity = group_identity_inner_range(GroupId(group)).with_prefix(prefix.clone());

			for keyspace in DATA_KEYSPACES {
				let key = OperatorStateKey::new(node, GroupId(group), keyspace, vec![7]).encode();
				assert!(contains(&data, key.as_slice()));
				assert!(!contains(&identity, key.as_slice()));
			}
			for keyspace in IDENTITY_KEYSPACES {
				let key = OperatorStateKey::new(node, GroupId(group), keyspace, vec![7]).encode();
				assert!(contains(&identity, key.as_slice()));
				assert!(!contains(&data, key.as_slice()));
			}
		}
	}

	#[test]
	fn decode_inner_round_trips_the_tail() {
		let key = OperatorStateKey::new(FlowNodeId(3), GroupId(77), Keyspace::EMIT, vec![4, 5, 6]);
		let (group, keyspace, suffix) =
			OperatorStateKey::decode_inner(key.inner().as_slice()).expect("inner must decode");

		assert_eq!(group, GroupId(77));
		assert_eq!(keyspace, Keyspace::EMIT);
		assert_eq!(suffix, vec![4, 5, 6]);
	}

	#[test]
	fn interned_group_keys_stay_compact() {
		// Interning exists to buy contiguity without paying for it in key bytes: raw group bytes
		// would put two base58 addresses in every accumulator key. A regression here means the
		// substrate is inflating the very state the plan set out to shrink.
		let interned =
			OperatorStateKey::new(FlowNodeId(17), GroupId(123_456), Keyspace::ACCUMULATOR, vec![0; 8])
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
}
