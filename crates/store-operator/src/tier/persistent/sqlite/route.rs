// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			keyspace::{KEYSPACES, KeyspaceVisitor, columns_width, dispatch},
			state::{GroupId, KeyspaceId, OperatorStateKey},
			traits::Keyspace,
		},
		typed::{layout::KeyLayout, range::KeyRange},
	},
	state::typed::SuffixBytes,
};
use reifydb_value::byte_size::ByteSize;
use rusqlite::{Connection, Transaction};

const GROUP_BYTES: usize = 16;

use crate::{tier::persistent::sqlite::typed, types::OperatorStateCensus};

fn parts(key: &EncodedKey) -> (GroupId, KeyspaceId, Vec<u8>) {
	OperatorStateKey::decode_inner(key.as_slice())
		.expect("an operator state key must name a group and a keyspace")
}

fn encode<K: Keyspace>(key: &K::Key) -> EncodedKey {
	let (group, suffix) = K::split(key);
	OperatorStateKey::inner_encoded(group, K::ID, suffix.to_suffix_bytes()).into_encoded()
}

fn typed_key<K: Keyspace>(group: GroupId, suffix: &[u8], fill: u8) -> K::Key {
	let mut bytes = suffix.to_vec();
	bytes.resize(columns_width(<K::Suffix as KeyLayout>::COLUMNS), fill);
	K::join(
		group,
		<K::Suffix as SuffixBytes>::from_suffix_bytes(&bytes)
			.expect("an operator state suffix must decode as its own keyspace layout"),
	)
}

struct Get<'a> {
	conn: &'a Connection,
	operator: OperatorId,
	group: GroupId,
	suffix: &'a [u8],
}

impl KeyspaceVisitor for Get<'_> {
	type Output = Option<Vec<u8>>;

	fn visit<K: Keyspace>(self) -> Self::Output {
		typed::get::<K>(self.conn, self.operator, &typed_key::<K>(self.group, self.suffix, 0x00))
	}
}

pub(super) fn get(conn: &Connection, operator: OperatorId, key: &EncodedKey) -> Option<Vec<u8>> {
	let (group, keyspace, suffix) = parts(key);
	dispatch(
		keyspace,
		Get {
			conn,
			operator,
			group,
			suffix: &suffix,
		},
	)
	.expect("an operator state key must name a keyspace in the catalogue")
}

struct Bounded<'a> {
	conn: &'a Connection,
	operator: OperatorId,
	group: Option<GroupId>,
	start: Bound<Vec<u8>>,
	end: Bound<Vec<u8>>,
	limit: u64,
	reverse: bool,
}

impl KeyspaceVisitor for Bounded<'_> {
	type Output = Vec<(EncodedKey, Vec<u8>)>;

	fn visit<K: Keyspace>(self) -> Self::Output {
		let start = match (self.group, &self.start) {
			(None, _) => Bound::Unbounded,
			(Some(group), Bound::Unbounded) => Bound::Included(typed_key::<K>(group, &[], 0x00)),
			(Some(group), Bound::Included(suffix)) => {
				Bound::Included(typed_key::<K>(group, suffix, 0x00))
			}
			(Some(group), Bound::Excluded(suffix)) => {
				Bound::Excluded(typed_key::<K>(group, suffix, 0x00))
			}
		};
		let end = match (self.group, &self.end) {
			(None, _) => Bound::Unbounded,
			(Some(group), Bound::Unbounded) => Bound::Included(typed_key::<K>(group, &[], 0xFF)),
			(Some(group), Bound::Included(suffix)) => {
				Bound::Included(typed_key::<K>(group, suffix, 0x00))
			}
			(Some(group), Bound::Excluded(suffix)) => {
				Bound::Excluded(typed_key::<K>(group, suffix, 0x00))
			}
		};
		let range = KeyRange::new(start, end);
		let rows = match self.reverse {
			false => typed::range::<K>(self.conn, self.operator, &range, self.limit),
			true => typed::last::<K>(self.conn, self.operator, &range, self.limit),
		};
		rows.into_iter().map(|(key, bytes)| (encode::<K>(&key), bytes)).collect()
	}
}

fn split_bound(bound: Bound<&EncodedKey>) -> (Bound<Vec<u8>>, Option<GroupId>, Option<KeyspaceId>) {
	let (key, wrap): (&EncodedKey, fn(Vec<u8>) -> Bound<Vec<u8>>) = match bound {
		Bound::Unbounded => return (Bound::Unbounded, None, None),
		Bound::Included(key) => (key, Bound::Included),
		Bound::Excluded(key) => (key, Bound::Excluded),
	};
	let bytes = key.as_slice();
	assert!(bytes.len() >= GROUP_BYTES, "an operator state range bound must at least name its group");
	if bytes.len() == GROUP_BYTES {
		if matches!(bound, Bound::Excluded(_)) {
			return (Bound::Unbounded, None, None);
		}
		let mut padded = bytes.to_vec();
		padded.push(0);
		let (group, _, _) = parts(&EncodedKey::new(padded));
		return (Bound::Unbounded, Some(group), None);
	}
	let (group, keyspace, suffix) = parts(key);
	(wrap(suffix), Some(group), Some(keyspace))
}

fn span(start: Option<KeyspaceId>, end: Option<KeyspaceId>, end_open: bool) -> Vec<KeyspaceId> {
	let high = start.map(|id| id.0).unwrap_or(u8::MAX);
	let low = match (end, end_open) {
		(Some(id), true) => id.0.saturating_add(1),
		(Some(id), false) => id.0,
		(None, _) => 0,
	};
	let mut ids: Vec<KeyspaceId> =
		KEYSPACES.iter().map(|spec| spec.id).filter(|id| id.0 <= high && id.0 >= low).collect();
	ids.sort_by(|left, right| right.0.cmp(&left.0));
	ids
}

pub(super) fn bounded(
	conn: &Connection,
	operator: OperatorId,
	range: &EncodedKeyRange,
	limit: u64,
	reverse: bool,
) -> Vec<(EncodedKey, Vec<u8>)> {
	let (start, start_group, start_at) = split_bound(range.start.as_ref());
	let (end, end_group, end_at) = split_bound(range.end.as_ref());
	let group = start_group.or(end_group);
	if let (Some(first), Some(second)) = (start_group, end_group) {
		assert_eq!(first, second, "an operator state range must not span groups");
	}
	let end_open = matches!(end, Bound::Excluded(ref suffix) if suffix.is_empty());
	let mut ids = span(start_at, end_at, end_open);
	if reverse {
		ids.reverse();
	}

	let mut out = Vec::new();
	let mut remaining = limit;
	for id in ids {
		if remaining == 0 {
			break;
		}
		let rows = dispatch(
			id,
			Bounded {
				conn,
				operator,
				group,
				start: match Some(id) == start_at {
					true => start.clone(),
					false => Bound::Unbounded,
				},
				end: match Some(id) == end_at && !end_open {
					true => end.clone(),
					false => Bound::Unbounded,
				},
				limit: remaining,
				reverse,
			},
		)
		.expect("an operator state range must name a keyspace in the catalogue");
		remaining -= rows.len().min(remaining as usize) as u64;
		out.extend(rows);
	}
	out
}

struct Drop<'a> {
	txn: &'a Transaction<'a>,
	operator: OperatorId,
}

impl KeyspaceVisitor for Drop<'_> {
	type Output = ();

	fn visit<K: Keyspace>(self) -> Self::Output {
		typed::drop_operator_in::<K>(self.txn, self.operator);
	}
}

pub(super) fn drop_operator(txn: &Transaction, operator: OperatorId) {
	for spec in KEYSPACES {
		dispatch(
			spec.id,
			Drop {
				txn,
				operator,
			},
		)
		.expect("every catalogue entry must dispatch to its own keyspace");
	}
}

struct Census<'a> {
	conn: &'a Connection,
}

impl KeyspaceVisitor for Census<'_> {
	type Output = Vec<OperatorStateCensus>;

	fn visit<K: Keyspace>(self) -> Self::Output {
		let width = columns_width(<K::Key as KeyLayout>::COLUMNS) as u64;
		typed::census::<K>(self.conn)
			.into_iter()
			.map(|(operator, keys, value_bytes)| OperatorStateCensus {
				operator,
				keyspace: K::ID,
				keys,
				key_bytes: ByteSize::from_bytes(keys * width),
				value_bytes: ByteSize::from_bytes(value_bytes),
			})
			.collect()
	}
}

pub(super) fn census(conn: &Connection) -> Vec<OperatorStateCensus> {
	let mut out = Vec::new();
	for spec in KEYSPACES {
		out.extend(
			dispatch(
				spec.id,
				Census {
					conn,
				},
			)
			.expect("every catalogue entry must dispatch to its own keyspace"),
		);
	}
	out
}
