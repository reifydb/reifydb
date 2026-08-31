// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::key::operator::{
	keyspace::KEYSPACES,
	state::{GroupId, KeyspaceId, OperatorStateKey},
};

const GROUP_BYTES: usize = 16;

pub(crate) fn parts(key: &EncodedKey) -> (GroupId, KeyspaceId, Vec<u8>) {
	OperatorStateKey::decode_inner(key.as_slice()).expect("an operator state key must name a group and a keyspace")
}

fn group_at(bytes: &[u8]) -> GroupId {
	let mut padded = bytes.to_vec();
	padded.resize(GROUP_BYTES, 0);
	padded.push(0);
	parts(&EncodedKey::new(padded)).0
}

fn group_below(bytes: &[u8]) -> GroupId {
	let mut padded = bytes.to_vec();
	padded.resize(GROUP_BYTES, 0);
	for index in (0..GROUP_BYTES).rev() {
		if padded[index] == 0 {
			padded[index] = u8::MAX;
			continue;
		}
		padded[index] -= 1;
		break;
	}
	padded.push(0);
	parts(&EncodedKey::new(padded)).0
}

pub(crate) fn split_bound(bound: Bound<&EncodedKey>) -> (Bound<Vec<u8>>, Option<GroupId>, Option<KeyspaceId>) {
	let (key, wrap): (&EncodedKey, fn(Vec<u8>) -> Bound<Vec<u8>>) = match bound {
		Bound::Unbounded => return (Bound::Unbounded, None, None),
		Bound::Included(key) => (key, Bound::Included),
		Bound::Excluded(key) => (key, Bound::Excluded),
	};
	let bytes = key.as_slice();
	if bytes.is_empty() {
		return (Bound::Unbounded, None, None);
	}
	if bytes.len() <= GROUP_BYTES {
		let group = match bound {
			Bound::Excluded(_) => group_below(bytes),
			_ => group_at(bytes),
		};
		return (Bound::Unbounded, Some(group), None);
	}
	let (group, keyspace, suffix) = parts(key);
	(wrap(suffix), Some(group), Some(keyspace))
}

pub(crate) fn span(start: Option<KeyspaceId>, end: Option<KeyspaceId>, end_open: bool) -> Vec<KeyspaceId> {
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
