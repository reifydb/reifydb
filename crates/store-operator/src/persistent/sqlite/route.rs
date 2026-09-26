// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Reverse,
	collections::{BTreeMap, HashMap},
	ops::Bound,
};

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			keyspace::{KEYSPACES, KeyspaceVisitor, dispatch},
			state::{GroupId, KeyspaceId, OperatorStateKey},
			traits::{Keyspace, group_scoped},
		},
		typed::{BoundedKey, layout::KeyLayout, range::KeyRange},
	},
	state::typed::SuffixBytes,
};
use rusqlite::{Connection, Transaction};

use crate::{
	bound::{KeyspaceIds, parts, span, split_bound},
	persistent::sqlite::{census::Census, registry::TableMask, typed},
	store::occupancy::occupies,
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
	<K::Suffix as BoundedKey>::low()
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
	tables: TableMask,
	group: GroupId,
	suffix: &'a [u8],
}

impl KeyspaceVisitor for Get<'_> {
	type Output = Option<Vec<u8>>;

	fn visit<K: Keyspace>(self) -> Self::Output {
		if !self.tables.holds(K::ID) {
			return None;
		}
		if !const { group_scoped::<K>() } && !self.group.is_root() {
			return None;
		}
		typed::get::<K>(self.conn, self.operator, &typed_key::<K>(self.group, self.suffix, lowest::<K>()))
	}
}

type GetManyItem<'a> = (&'a EncodedKey, GroupId, &'a [u8]);

struct GetMany<'a> {
	conn: &'a Connection,
	operator: OperatorId,
	tables: TableMask,
	items: Vec<GetManyItem<'a>>,
}

impl KeyspaceVisitor for GetMany<'_> {
	type Output = Vec<(EncodedKey, Vec<u8>)>;

	fn visit<K: Keyspace>(self) -> Self::Output {
		if !self.tables.holds(K::ID) {
			return Vec::new();
		}
		let mut probes = Vec::with_capacity(self.items.len());
		let mut origin: HashMap<EncodedKey, EncodedKey> = HashMap::with_capacity(self.items.len());
		for (key, group, suffix) in self.items {
			if !const { group_scoped::<K>() } && !group.is_root() {
				continue;
			}
			let typed = typed_key::<K>(group, suffix, lowest::<K>());
			origin.insert(encode::<K>(&typed), key.clone());
			probes.push(typed);
		}
		typed::get_batch::<K>(self.conn, self.operator, &probes)
			.into_iter()
			.filter_map(|(typed, bytes)| origin.get(&encode::<K>(&typed)).map(|key| (key.clone(), bytes)))
			.collect()
	}
}

pub(super) fn get_many(
	conn: &Connection,
	operator: OperatorId,
	tables: TableMask,
	keys: &[EncodedKey],
) -> Vec<(EncodedKey, Vec<u8>)> {
	let mut grouped: BTreeMap<KeyspaceId, Vec<GetManyItem>> = BTreeMap::new();
	for key in keys {
		let (group, keyspace, suffix) = parts(key);
		grouped.entry(keyspace).or_default().push((key, group, suffix));
	}
	let mut out = Vec::with_capacity(keys.len());
	for (keyspace, items) in grouped {
		let found = dispatch(
			keyspace,
			GetMany {
				conn,
				operator,
				tables,
				items,
			},
		)
		.expect("an operator state key must name a keyspace in the catalogue");
		out.extend(found);
	}
	out
}

pub(super) fn get(conn: &Connection, operator: OperatorId, tables: TableMask, key: &EncodedKey) -> Option<Vec<u8>> {
	let (group, keyspace, suffix) = parts(key);
	dispatch(
		keyspace,
		Get {
			conn,
			operator,
			tables,
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
	ids.sort_by_key(|id| Reverse(id.0));
	ids
}

pub(super) fn bounded(
	conn: &Connection,
	operator: OperatorId,
	tables: TableMask,
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
	ids.retain(|id| tables.holds(*id));
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

struct GroupSet<'a> {
	conn: &'a Connection,
	operator: OperatorId,
	groups: &'a [GroupId],
	root: bool,
	start: Bound<Vec<u8>>,
	end: Bound<Vec<u8>>,
	limit: u64,
	reverse: bool,
}

impl KeyspaceVisitor for GroupSet<'_> {
	type Output = Vec<(EncodedKey, Vec<u8>)>;

	fn visit<K: Keyspace>(self) -> Self::Output {
		let grouped = const { group_scoped::<K>() };
		if !grouped && !self.root {
			return Vec::new();
		}
		let start = match &self.start {
			Bound::Unbounded => Bound::Unbounded,
			Bound::Included(suffix) => {
				Bound::Included(typed_key::<K>(GroupId::ROOT, suffix, lowest::<K>()))
			}
			Bound::Excluded(suffix) => {
				Bound::Excluded(typed_key::<K>(GroupId::ROOT, suffix, highest::<K>()))
			}
		};
		let end = match &self.end {
			Bound::Unbounded => Bound::Unbounded,
			Bound::Included(suffix) => {
				Bound::Included(typed_key::<K>(GroupId::ROOT, suffix, highest::<K>()))
			}
			Bound::Excluded(suffix) => {
				Bound::Excluded(typed_key::<K>(GroupId::ROOT, suffix, lowest::<K>()))
			}
		};
		let range = KeyRange::new(start, end);
		let rows = match (grouped, self.reverse) {
			(true, false) => {
				typed::range_in::<K>(self.conn, self.operator, self.groups, &range, self.limit, "ASC")
			}
			(true, true) => {
				typed::range_in::<K>(self.conn, self.operator, self.groups, &range, self.limit, "DESC")
			}
			(false, false) => typed::range::<K>(self.conn, self.operator, &range, self.limit),
			(false, true) => typed::last::<K>(self.conn, self.operator, &range, self.limit),
		};
		rows.into_iter().map(|(key, bytes)| (encode::<K>(&key), bytes)).collect()
	}
}

fn window(at: Option<KeyspaceId>, id: KeyspaceId, suffix: &Bound<Vec<u8>>) -> Bound<Vec<u8>> {
	match at {
		Some(at) if at == id => suffix.clone(),
		_ => Bound::Unbounded,
	}
}

pub(super) fn bounded_in(
	conn: &Connection,
	operator: OperatorId,
	groups: &[GroupId],
	range: &EncodedKeyRange,
	limit: u64,
	reverse: bool,
	mask: u64,
) -> Vec<(EncodedKey, Vec<u8>)> {
	if groups.is_empty() {
		return Vec::new();
	}
	let (start, _, start_at) = split_bound(range.start.as_ref());
	let (end, _, end_at) = split_bound(range.end.as_ref());
	let end_open = matches!(end, Bound::Excluded(ref suffix) if suffix.is_empty());
	let mut ids = span(start_at, end_at, end_open);
	ids.retain(|id| occupies(mask, *id));
	if reverse {
		ids.reverse();
	}
	let one_group = groups.len() == 1;
	let root = groups.iter().any(GroupId::is_root);

	let mut out: Vec<(EncodedKey, Vec<u8>)> = Vec::new();
	let mut remaining = limit;
	for id in ids {
		if one_group && remaining == 0 {
			break;
		}
		let rows = dispatch(
			id,
			GroupSet {
				conn,
				operator,
				groups,
				root,
				start: window(start_at, id, &start),
				end: window(end_at, id, &end),
				limit: match one_group {
					true => remaining,
					false => limit,
				},
				reverse,
			},
		)
		.expect("an operator state group sweep must name a keyspace in the catalogue");
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

struct Clear<'a> {
	txn: &'a Transaction<'a>,
	operator: OperatorId,
}

impl KeyspaceVisitor for Clear<'_> {
	type Output = ();

	fn visit<K: Keyspace>(self) -> Self::Output {
		typed::clear::<K>(self.txn, self.operator)
	}
}

pub(super) fn drop_operator(txn: &Transaction, operator: OperatorId, tables: TableMask) {
	for keyspace in tables.held() {
		dispatch(
			keyspace,
			Clear {
				txn,
				operator,
			},
		)
		.expect("every catalogue entry must dispatch to its own keyspace");
	}
}

struct Occupied<'a> {
	conn: &'a Connection,
	operator: OperatorId,
}

impl KeyspaceVisitor for Occupied<'_> {
	type Output = bool;

	fn visit<K: Keyspace>(self) -> Self::Output {
		typed::occupied::<K>(self.conn, self.operator)
	}
}

pub(super) fn occupied_keyspaces(conn: &Connection, operator: OperatorId, tables: TableMask) -> Vec<KeyspaceId> {
	tables.held()
		.into_iter()
		.filter(|keyspace| {
			dispatch(
				*keyspace,
				Occupied {
					conn,
					operator,
				},
			)
			.expect("every catalogue entry must dispatch to its own keyspace")
		})
		.collect()
}

struct KeysAfter<'a> {
	conn: &'a Connection,
	operator: OperatorId,
	tables: TableMask,
	after: Option<&'a EncodedKey>,
	limit: u64,
}

impl KeyspaceVisitor for KeysAfter<'_> {
	type Output = Vec<EncodedKey>;

	fn visit<K: Keyspace>(self) -> Self::Output {
		if !self.tables.holds(K::ID) {
			return Vec::new();
		}
		let cursor = self.after.map(|key| {
			let (group, _, suffix) = parts(key);
			typed_key::<K>(group, suffix, lowest::<K>())
		});
		typed::keys_after::<K>(self.conn, self.operator, cursor.as_ref(), self.limit)
			.iter()
			.map(encode::<K>)
			.collect()
	}
}

pub(super) fn keys_after(
	conn: &Connection,
	operator: OperatorId,
	tables: TableMask,
	keyspace: KeyspaceId,
	after: Option<&EncodedKey>,
	limit: u64,
) -> Vec<EncodedKey> {
	dispatch(
		keyspace,
		KeysAfter {
			conn,
			operator,
			tables,
			after,
			limit,
		},
	)
	.expect("an operator state keyspace must appear in the catalogue")
}

pub(super) fn census(conn: &Connection, tables: &HashMap<OperatorId, TableMask>) -> Vec<OperatorStateCensus> {
	let mut out = Vec::new();
	for (operator, mask) in tables {
		for keyspace in mask.held() {
			let entry = dispatch(
				keyspace,
				Census {
					conn,
					operator: *operator,
				},
			)
			.expect("every catalogue entry must dispatch to its own keyspace");
			out.extend(entry);
		}
	}
	out.sort_by_key(|entry| (entry.operator, entry.keyspace));
	out
}

#[cfg(test)]
mod tests {
	use std::cmp::Reverse;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::{
			operator::{
				keyspace::{
					distinct::{DistinctEntry, DistinctEntryKey},
					expiry::{Expiry, ExpiryKey},
					join::{JoinLeft, JoinLeftKey, JoinRight, JoinRightKey},
				},
				state::{GroupId, KeyspaceId, group_inner_range},
			},
			typed::direction::{Asc, Desc},
		},
	};
	use reifydb_value::{util::hash::Hash128, value::row_number::RowNumber};

	use crate::{
		bound::parts,
		persistent::sqlite::{
			SqlitePersistent,
			fixture::{open, set_one},
		},
		types::OperatorBatch,
	};

	const OPERATOR: OperatorId = OperatorId(1);

	const LIMIT: u64 = 1024;

	fn group(id: u128) -> GroupId {
		GroupId::hashed(Hash128(id))
	}

	fn encoded_order(groups: &[GroupId]) -> Vec<GroupId> {
		let mut ordered = groups.to_vec();
		ordered.sort_by_key(|group| Reverse(*group.as_bytes()));
		ordered
	}

	fn seed(store: &SqlitePersistent, groups: &[GroupId]) {
		for group in groups {
			for row in [1u64, 2] {
				set_one::<JoinLeft>(
					store,
					OPERATOR,
					&JoinLeftKey {
						group: Desc(*group),
						row: Asc(RowNumber(row)),
					},
					b"left",
				);
			}
			set_one::<JoinRight>(
				store,
				OPERATOR,
				&JoinRightKey {
					group: Desc(*group),
					row: Asc(RowNumber(1)),
				},
				b"right",
			);
			set_one::<DistinctEntry>(
				store,
				OPERATOR,
				&DistinctEntryKey {
					group: Desc(*group),
				},
				b"distinct",
			);
		}
	}

	fn seed_groupless(store: &SqlitePersistent) {
		set_one::<Expiry>(
			store,
			OPERATOR,
			&ExpiryKey {
				threshold: Desc(7),
				owner: Desc(Hash128(9)),
			},
			b"expiry",
		);
	}

	fn keys(batch: OperatorBatch) -> Vec<EncodedKey> {
		batch.items.into_iter().map(|(key, _)| key.into_encoded()).collect()
	}

	fn sweep(store: &SqlitePersistent, groups: &[GroupId]) -> Vec<EncodedKey> {
		keys(store.group_page(OPERATOR, groups, LIMIT, u64::MAX))
	}

	#[test]
	fn a_group_set_sweep_answers_with_exactly_what_the_single_group_sweeps_answer_with() {
		// a row the per group fan out finds but the set misses leaves the reaper deleting less than it swept
		let store = open();
		let groups = [group(11), group(22), group(33)];
		seed(&store, &groups);

		let mut expected: Vec<EncodedKey> = Vec::new();
		for group in encoded_order(&groups) {
			expected.extend(keys(store.range_batch(OPERATOR, group_inner_range(group), LIMIT)));
		}

		assert_eq!(sweep(&store, &encoded_order(&groups)), expected);
		assert_eq!(expected.len(), 12, "three groups each hold two join left, one join right and one distinct");
	}

	#[test]
	fn a_group_set_sweep_answers_in_encoded_key_order() {
		// the per keyspace queries arrive keyspace major while the encoded key is group major, so without the
		// merge sort a caller paging the answer skips whole groups
		let store = open();
		let groups = [group(11), group(22), group(33)];
		seed(&store, &groups);

		let swept = sweep(&store, &groups);
		let mut sorted = swept.clone();
		sorted.sort_by(|left, right| left.as_slice().cmp(right.as_slice()));

		assert_eq!(swept, sorted);
	}

	#[test]
	fn a_group_outside_the_set_is_never_answered_with() {
		// the group predicate is the only thing keeping a neighbour out, and a wrong one hands the reaper rows
		// of a group that is still live
		let store = open();
		let groups = [group(11), group(22), group(33)];
		seed(&store, &groups);

		let asked = [group(11), group(33)];
		let swept = sweep(&store, &asked);

		assert!(!swept.is_empty());
		for key in &swept {
			let (found, _, _) = parts(key);
			assert!(asked.contains(&found), "{found} was never asked for");
		}
		assert_eq!(swept.len(), 8);
	}

	#[test]
	fn a_groupless_keyspace_answers_only_when_root_is_in_the_set() {
		// a groupless keyspace has no group column to filter on, so any set without root must skip it rather
		// than hand back the same root rows every sweep
		let store = open();
		let groups = [group(11), group(22)];
		seed(&store, &groups);
		seed_groupless(&store);

		let rolling = |key: &EncodedKey| parts(key).1 == KeyspaceId::ROLLING_EXPIRY;

		assert!(!sweep(&store, &groups).iter().any(rolling));
		assert!(sweep(&store, &[GroupId::ROOT, group(11)]).iter().any(rolling));
	}

	#[test]
	fn a_one_group_set_sweep_matches_the_single_group_range_sweep() {
		// a lone group must answer exactly as the range path does, otherwise the collapse changes every sweep
		// that never batched
		let store = open();
		let groups = [group(11), group(22)];
		seed(&store, &groups);

		let expected = keys(store.range_batch(OPERATOR, group_inner_range(group(11)), LIMIT));

		assert_eq!(sweep(&store, &[group(11)]), expected);
		assert_eq!(expected.len(), 4);
	}

	#[test]
	fn an_empty_group_set_answers_with_nothing() {
		// an unbounded IN list would degrade to a full keyspace scan, so naming no group must name no row
		let store = open();
		seed(&store, &[group(11)]);

		assert!(sweep(&store, &[]).is_empty());
	}
}
