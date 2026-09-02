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
			traits::{Keyspace, group_scoped},
		},
		typed::{TypedKey, layout::KeyLayout, range::KeyRange},
	},
	state::typed::SuffixBytes,
};
use reifydb_value::byte_size::ByteSize;
use rusqlite::{Connection, Transaction};

use crate::{
	tier::{
		bound::{KeyspaceIds, parts, span, split_bound},
		persistent::sqlite::typed,
	},
	types::OperatorStateCensus,
};

fn encode<K: Keyspace>(key: &K::GroupedKey) -> EncodedKey {
	let (group, suffix) = K::split(key);
	OperatorStateKey::inner_encoded(group, K::ID, suffix.to_suffix_bytes()).into_encoded()
}

fn typed_key<K: Keyspace>(group: GroupId, suffix: &[u8], edge: K::Suffix) -> K::GroupedKey {
	let template = edge.to_suffix_bytes();
	let mut bytes = suffix.to_vec();
	bytes.truncate(template.len());
	bytes.extend_from_slice(&template[bytes.len()..]);
	K::join(
		group,
		<K::Suffix as SuffixBytes>::from_suffix_bytes(&bytes)
			.expect("an operator state suffix must decode as its own keyspace layout"),
	)
}

fn lowest<K: Keyspace>() -> K::Suffix {
	<K::Suffix as TypedKey>::low()
}

fn highest<K: Keyspace>() -> K::Suffix {
	<K::Suffix as KeyLayout>::high()
}

fn within(range: &EncodedKeyRange, key: &EncodedKey) -> bool {
	let after_start = match &range.start {
		Bound::Unbounded => true,
		Bound::Included(start) => key.as_slice() >= start.as_slice(),
		Bound::Excluded(start) => key.as_slice() > start.as_slice(),
	};
	let before_end = match &range.end {
		Bound::Unbounded => true,
		Bound::Included(end) => key.as_slice() <= end.as_slice(),
		Bound::Excluded(end) => key.as_slice() < end.as_slice(),
	};
	after_start && before_end
}

fn outside_root_only<K: Keyspace>(start: Option<GroupId>, end: Option<GroupId>) -> bool {
	match (const { group_scoped::<K>() }, start, end) {
		(false, Some(start), Some(end)) => !start.is_root() && !end.is_root(),
		_ => false,
	}
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
		if !const { group_scoped::<K>() } && !self.group.is_root() {
			return None;
		}
		typed::get::<K>(self.conn, self.operator, &typed_key::<K>(self.group, self.suffix, lowest::<K>()))
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
			suffix,
		},
	)
	.expect("an operator state key must name a keyspace in the catalogue")
}

struct Bounded<'a> {
	conn: &'a Connection,
	operator: OperatorId,
	start_group: Option<GroupId>,
	start: Bound<Vec<u8>>,
	end_group: Option<GroupId>,
	end: Bound<Vec<u8>>,
	full: &'a EncodedKeyRange,
	limit: u64,
	reverse: bool,
}

impl KeyspaceVisitor for Bounded<'_> {
	type Output = Vec<(EncodedKey, Vec<u8>)>;

	fn visit<K: Keyspace>(self) -> Self::Output {
		if outside_root_only::<K>(self.start_group, self.end_group) {
			return Vec::new();
		}
		let start = match (self.start_group, &self.start) {
			(None, _) => Bound::Unbounded,
			(Some(group), Bound::Unbounded) => Bound::Included(typed_key::<K>(group, &[], lowest::<K>())),
			(Some(group), Bound::Included(suffix)) => {
				Bound::Included(typed_key::<K>(group, suffix, lowest::<K>()))
			}
			(Some(group), Bound::Excluded(suffix)) => {
				Bound::Excluded(typed_key::<K>(group, suffix, highest::<K>()))
			}
		};
		let end = match (self.end_group, &self.end) {
			(None, _) => Bound::Unbounded,
			(Some(group), Bound::Unbounded) => Bound::Included(typed_key::<K>(group, &[], highest::<K>())),
			(Some(group), Bound::Included(suffix)) => {
				Bound::Included(typed_key::<K>(group, suffix, highest::<K>()))
			}
			(Some(group), Bound::Excluded(suffix)) => {
				Bound::Excluded(typed_key::<K>(group, suffix, lowest::<K>()))
			}
		};
		let range = KeyRange::new(start, end);
		let rows = match self.reverse {
			false => typed::range::<K>(self.conn, self.operator, &range, self.limit),
			true => typed::last::<K>(self.conn, self.operator, &range, self.limit),
		};
		let full = self.full;
		let inside_one_group = const { group_scoped::<K>() };
		rows.into_iter()
			.map(|(key, bytes)| (encode::<K>(&key), bytes))
			.filter(|(key, _)| inside_one_group || within(full, key))
			.collect()
	}
}

fn keyspace_start(
	id: KeyspaceId,
	group: Option<GroupId>,
	at: Option<KeyspaceId>,
	suffix: &Bound<Vec<u8>>,
) -> Option<(Option<GroupId>, Bound<Vec<u8>>)> {
	let Some(group) = group else {
		return Some((None, Bound::Unbounded));
	};
	let Some(at) = at else {
		return Some((Some(group), Bound::Unbounded));
	};
	if id == at {
		return Some((Some(group), suffix.clone()));
	}
	if id.0 < at.0 {
		return Some((Some(group), Bound::Unbounded));
	}
	group.predecessor().map(|next| (Some(next), Bound::Unbounded))
}

fn keyspace_end(
	id: KeyspaceId,
	group: Option<GroupId>,
	at: Option<KeyspaceId>,
	suffix: &Bound<Vec<u8>>,
) -> Option<(Option<GroupId>, Bound<Vec<u8>>)> {
	let Some(group) = group else {
		return Some((None, Bound::Unbounded));
	};
	let Some(at) = at else {
		return Some((Some(group), Bound::Unbounded));
	};
	if id == at {
		return Some((Some(group), suffix.clone()));
	}
	if id.0 > at.0 {
		return Some((Some(group), Bound::Unbounded));
	}
	group.successor().map(|previous| (Some(previous), Bound::Unbounded))
}

fn every_keyspace() -> KeyspaceIds {
	let mut ids: KeyspaceIds = KEYSPACES.iter().map(|spec| spec.id).collect();
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
	let end_open = matches!(end, Bound::Excluded(ref suffix) if suffix.is_empty());
	let one_group = matches!((start_group, end_group), (Some(first), Some(second)) if first == second);

	let mut ids = match one_group {
		true => span(start_at, end_at, end_open),
		false => every_keyspace(),
	};
	if reverse {
		ids.reverse();
	}

	let mut out: Vec<(EncodedKey, Vec<u8>)> = Vec::new();
	let mut remaining = limit;
	for id in ids {
		if one_group && remaining == 0 {
			break;
		}
		let (Some((keyspace_start_group, keyspace_start)), Some((keyspace_end_group, keyspace_end))) =
			(keyspace_start(id, start_group, start_at, &start), keyspace_end(id, end_group, end_at, &end))
		else {
			continue;
		};
		let rows = dispatch(
			id,
			Bounded {
				conn,
				operator,
				start_group: keyspace_start_group,
				start: keyspace_start,
				end_group: keyspace_end_group,
				end: keyspace_end,
				full: range,
				limit: match one_group {
					true => remaining,
					false => limit,
				},
				reverse,
			},
		)
		.expect("an operator state range must name a keyspace in the catalogue");
		remaining -= rows.len().min(remaining as usize) as u64;
		out.extend(rows);
	}
	if !one_group {
		out.sort_by(|left, right| match reverse {
			false => left.0.as_slice().cmp(right.0.as_slice()),
			true => right.0.as_slice().cmp(left.0.as_slice()),
		});
		out.truncate(limit as usize);
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
		let width = columns_width(<K::GroupedKey as KeyLayout>::COLUMNS) as u64;
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
		out.extend(dispatch(
			spec.id,
			Census {
				conn,
			},
		)
		.expect("every catalogue entry must dispatch to its own keyspace"));
	}
	out
}
