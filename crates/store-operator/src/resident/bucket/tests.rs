// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			keyspace::{
				KEYSPACES, KeyspaceVisitor, dispatch,
				join::{JoinLeft, JoinRight},
			},
			state::{GroupId, KeyspaceId, OperatorStateKey, group_data_inner_range},
			traits::Keyspace,
		},
		typed::direction::Asc,
	},
	state::typed::SuffixBytes,
};
use reifydb_value::{byte_size::ByteSize, util::hash::Hash128, value::row_number::RowNumber};

use super::{
	BucketMap,
	write::{Staged, StandardBucket},
};
use crate::types::Scan;

const OP: OperatorId = OperatorId(1);

fn bucket() -> StandardBucket<JoinLeft> {
	StandardBucket::<JoinLeft>::new(OP)
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn suffix(n: u64) -> Asc<RowNumber> {
	Asc(RowNumber(n))
}

#[test]
fn a_write_bucket_reads_back_what_it_recorded_without_being_proven() {
	let mut bucket = bucket();
	bucket.record(GroupId::hashed(Hash128(7)), suffix(1), Some(row("written")));

	let entry =
		bucket.get(GroupId::hashed(Hash128(7)), &suffix(1)).expect("a recorded write must be readable at once");
	assert_eq!(
		String::from_utf8(entry.post.as_ref().expect("a set write keeps its row").body().to_vec())
			.expect("utf8"),
		"written",
		"the commit buffer is authoritative for its own writes; unlike the read cache it never reports a gap"
	);
}

#[test]
fn two_groups_in_one_write_bucket_never_read_each_other() {
	let mut bucket = bucket();
	bucket.record(GroupId::hashed(Hash128(7)), suffix(1), Some(row("seven")));
	bucket.record(GroupId::hashed(Hash128(9)), suffix(1), Some(row("nine")));

	for (group, expected) in [(7u128, "seven"), (9, "nine")] {
		let entry = bucket
			.get(GroupId::hashed(Hash128(group)), &suffix(1))
			.expect("each group holds its own suffix");
		assert_eq!(
			String::from_utf8(entry.post.as_ref().expect("a set write keeps its row").body().to_vec())
				.expect("utf8"),
			expected,
			"the group partitions the bucket, so one group's suffix must never answer for another's"
		);
	}
}

#[test]
fn a_write_bucket_ranges_its_suffixes_in_the_order_the_key_type_declares() {
	let mut bucket = bucket();
	for n in [3u64, 1, 2] {
		bucket.record(GroupId::hashed(Hash128(7)), suffix(n), Some(row(&format!("v{n}"))));
	}

	let order: Vec<u64> = bucket.range(GroupId::hashed(Hash128(7)), ..).map(|(key, _)| key.0.0).collect();
	assert_eq!(
		order,
		vec![1, 2, 3],
		"the flush merges the bucket against a sorted page, so out of order iteration corrupts the merge"
	);
}

#[test]
fn a_tombstone_is_recorded_rather_than_dropped() {
	let mut bucket = bucket();
	bucket.record(GroupId::hashed(Hash128(7)), suffix(1), Some(row("live")));
	bucket.record(GroupId::hashed(Hash128(7)), suffix(1), None);

	let entry = bucket
		.get(GroupId::hashed(Hash128(7)), &suffix(1))
		.expect("a removal must stay visible until it is flushed");
	assert!(entry.post.is_none(), "a removal is a tombstone, not an absence; dropping it would resurrect the row");
}

#[test]
fn overwriting_a_suffix_does_not_count_it_twice() {
	let mut bucket = bucket();
	bucket.record(GroupId::hashed(Hash128(7)), suffix(1), Some(row("first")));
	let after_first = bucket.footprint();
	bucket.record(GroupId::hashed(Hash128(7)), suffix(1), Some(row("first")));

	assert_eq!(bucket.len(), 1, "one suffix written twice is one row");
	assert_eq!(
		bucket.footprint(),
		after_first,
		"the flush budget is driven by the footprint, so double counting an overwrite starves it"
	);
}

#[test]
fn the_bucket_map_hands_back_the_same_bucket_for_one_operator_and_keyspace() {
	let mut map = BucketMap::default();

	map.bucket::<JoinLeft>(OP).record(GroupId::hashed(Hash128(7)), suffix(1), Some(row("first")));
	let entry = map
		.bucket::<JoinLeft>(OP)
		.get(GroupId::hashed(Hash128(7)), &suffix(1))
		.expect("the second lookup must reach the first bucket");

	assert_eq!(
		String::from_utf8(entry.post.as_ref().expect("a set write keeps its row").body().to_vec())
			.expect("utf8"),
		"first"
	);
}

#[test]
fn two_operators_never_share_a_bucket() {
	let mut map = BucketMap::default();
	let other = OperatorId(2);

	map.bucket::<JoinLeft>(OP).record(GroupId::hashed(Hash128(7)), suffix(1), Some(row("mine")));

	assert!(
		map.bucket::<JoinLeft>(other).get(GroupId::hashed(Hash128(7)), &suffix(1)).is_none(),
		"the bucket is keyed on operator and keyspace, so one operator's state must never answer for another's"
	);
}

#[test]
fn a_staged_row_is_not_staged_again_by_the_next_flush() {
	let mut bucket = bucket();
	bucket.record(GroupId::hashed(Hash128(7)), suffix(1), Some(row("seven")));
	bucket.record(GroupId::hashed(Hash128(9)), suffix(2), Some(row("nine")));

	let first = stage_rows(&mut bucket).len();
	let second = stage_rows(&mut bucket).len();

	assert_eq!(first, 2, "every dirty row in every group must be staged, not just the first group's");
	assert_eq!(second, 0, "a staged row must leave the dirty set or the next flush writes it twice");
	assert_eq!(bucket.dirty_len(), 0, "a staged row still counted dirty is written again by the next flush");
	assert_eq!(
		bucket.dirty_footprint(),
		ByteSize::ZERO,
		"the dirty footprint drives the flush budget, so a stage that does not release it never lets the budget recover"
	);
}

#[test]
fn an_erased_write_reaches_the_same_bucket_a_typed_one_does() {
	let mut map = BucketMap::default();
	map.record_bytes(
		OP,
		JoinLeft::ID,
		GroupId::hashed(Hash128(7)),
		&suffix(1).to_suffix_bytes(),
		Some(row("erased")),
	);

	let entry = map
		.bucket::<JoinLeft>(OP)
		.get(GroupId::hashed(Hash128(7)), &suffix(1))
		.expect("a byte keyed write must land in the typed bucket");
	assert_eq!(
		String::from_utf8(entry.post.as_ref().expect("a set write keeps its row").body().to_vec())
			.expect("utf8"),
		"erased",
		"the erased entry point is how every byte keyed caller reaches a bucket; if it misses, their writes vanish"
	);
}

#[test]
fn every_keyspace_in_the_catalogue_is_reachable_through_the_dispatch() {
	struct Name;

	impl KeyspaceVisitor for Name {
		type Output = &'static str;

		fn visit<K: Keyspace>(self) -> Self::Output {
			K::NAME
		}
	}

	for spec in KEYSPACES {
		assert_eq!(
			dispatch(spec.id, Name),
			Some(spec.name),
			"{} is in the catalogue but the dispatch cannot reach it, so every write to it would panic",
			spec.name
		);
	}

	let unclaimed = (0..=u8::MAX)
		.map(KeyspaceId)
		.find(|id| KEYSPACES.iter().all(|spec| spec.id != *id))
		.expect("the catalogue must leave at least one keyspace id unclaimed");
	assert_eq!(
		dispatch(unclaimed, Name),
		None,
		"an id no keyspace claims must report itself rather than answering as a neighbour"
	);
}

fn seeded_pair() -> BucketMap {
	let mut map = BucketMap::default();
	for group in [GroupId::hashed(Hash128(7)), GroupId::hashed(Hash128(9))] {
		for keyspace in [JoinLeft::ID, JoinRight::ID] {
			for n in [2u64, 1] {
				map.record_bytes(OP, keyspace, group, &suffix(n).to_suffix_bytes(), Some(row("v")));
			}
		}
	}
	map
}

fn expected_order() -> Vec<EncodedKey> {
	let mut keys = Vec::new();
	for group in [GroupId::hashed(Hash128(7)), GroupId::hashed(Hash128(9))] {
		for keyspace in [JoinLeft::ID, JoinRight::ID] {
			for n in [1u64, 2] {
				keys.push(OperatorStateKey::inner_encoded(
					group,
					keyspace,
					suffix(n).to_suffix_bytes(),
				)
				.into_encoded());
			}
		}
	}
	keys.sort();
	keys
}

#[test]
fn a_range_spanning_two_keyspaces_pages_without_skipping_a_key() {
	let map = seeded_pair();
	let whole: Vec<EncodedKey> = map
		.encoded_range(OP, &Bound::Unbounded, &Bound::Unbounded, Scan::Forward, usize::MAX)
		.into_iter()
		.map(|(key, _)| key.into_encoded())
		.collect();

	assert_eq!(whole, expected_order(), "an unbounded range must agree with an unbounded scan");

	let mut paged = Vec::new();
	let mut cursor = Bound::Unbounded;
	loop {
		let page = map.encoded_range(OP, &cursor, &Bound::Unbounded, Scan::Forward, usize::MAX);
		let Some((key, _)) = page.first() else {
			break;
		};
		paged.push(key.as_encoded().clone());
		cursor = Bound::Excluded(key.as_encoded().clone());
	}

	assert_eq!(paged, expected_order(), "walking one key at a time must visit every key exactly once");
}

#[test]
fn a_bounded_range_keeps_the_keys_between_its_bounds_and_no_others() {
	let map = seeded_pair();
	let all = expected_order();
	let lower = all[2].clone();
	let upper = all[5].clone();

	let seen: Vec<EncodedKey> = map
		.encoded_range(
			OP,
			&Bound::Included(lower.clone()),
			&Bound::Excluded(upper.clone()),
			Scan::Forward,
			usize::MAX,
		)
		.into_iter()
		.map(|(key, _)| key.into_encoded())
		.collect();

	assert_eq!(seen, all[2..5].to_vec(), "an included start belongs to the range and an excluded end does not");
}

#[test]
fn a_forward_page_stops_at_its_limit_and_keeps_the_leading_keys() {
	let map = seeded_pair();
	let all = expected_order();

	let page: Vec<EncodedKey> = map
		.encoded_range(OP, &Bound::Unbounded, &Bound::Unbounded, Scan::Forward, 3)
		.into_iter()
		.map(|(key, _)| key.into_encoded())
		.collect();

	assert_eq!(page, all[..3].to_vec(), "a forward page must be the first keys of the range, in order");
}

#[test]
fn a_backward_page_returns_the_trailing_keys_in_ascending_order() {
	let map = seeded_pair();
	let all = expected_order();

	let page: Vec<EncodedKey> = map
		.encoded_range(OP, &Bound::Unbounded, &Bound::Unbounded, Scan::Backward, 3)
		.into_iter()
		.map(|(key, _)| key.into_encoded())
		.collect();

	assert_eq!(page, all[5..].to_vec(), "a backward page must be the last keys of the range, still ascending");
}

#[test]
fn a_backward_page_that_crosses_a_group_stays_one_ascending_run() {
	let map = seeded_pair();
	let all = expected_order();

	let page: Vec<EncodedKey> = map
		.encoded_range(OP, &Bound::Unbounded, &Bound::Unbounded, Scan::Backward, 5)
		.into_iter()
		.map(|(key, _)| key.into_encoded())
		.collect();

	assert_eq!(page, all[3..].to_vec(), "a limit that spans two groups must still come back as one ascending run");
}

#[test]
fn a_limit_wider_than_the_range_returns_every_key() {
	let map = seeded_pair();

	let forward: Vec<EncodedKey> = map
		.encoded_range(OP, &Bound::Unbounded, &Bound::Unbounded, Scan::Forward, 99)
		.into_iter()
		.map(|(key, _)| key.into_encoded())
		.collect();
	let backward: Vec<EncodedKey> = map
		.encoded_range(OP, &Bound::Unbounded, &Bound::Unbounded, Scan::Backward, 99)
		.into_iter()
		.map(|(key, _)| key.into_encoded())
		.collect();

	assert_eq!(forward, expected_order(), "a limit past the end must not truncate the range");
	assert_eq!(backward, expected_order(), "a backward page wider than the range must still be ascending");
}

#[test]
fn a_zero_limit_returns_nothing_without_underflowing_the_remainder() {
	let map = seeded_pair();

	assert!(
		map.encoded_range(OP, &Bound::Unbounded, &Bound::Unbounded, Scan::Forward, 0).is_empty(),
		"a zero limit must take nothing"
	);
	assert!(
		map.encoded_range(OP, &Bound::Unbounded, &Bound::Unbounded, Scan::Backward, 0).is_empty(),
		"a zero limit must take nothing backward either"
	);
}

#[test]
fn a_reverse_scan_is_the_exact_mirror_of_a_forward_one() {
	let map = seeded_pair();
	let forward: Vec<EncodedKey> = map
		.encoded_range(OP, &Bound::Unbounded, &Bound::Unbounded, Scan::Forward, usize::MAX)
		.into_iter()
		.map(|(key, _)| key.into_encoded())
		.collect();

	let mut reverse = forward.clone();
	reverse.reverse();
	let mut expected = expected_order();
	expected.reverse();

	assert_eq!(reverse, expected, "a reverse walk must mirror the forward one key for key");
}

#[test]
fn a_whole_operator_scan_names_no_group_and_still_sweeps_every_one() {
	let map = seeded_pair();
	let empty = EncodedKey::new(Vec::new());

	let seen: Vec<EncodedKey> = map
		.encoded_range(
			OP,
			&Bound::Included(empty.clone()),
			&Bound::Excluded(empty.clone()),
			Scan::Forward,
			usize::MAX,
		)
		.into_iter()
		.map(|(key, _)| key.into_encoded())
		.collect();

	assert_eq!(seen, expected_order(), "an empty bound must sweep every group and keyspace the operator holds");
}

#[test]
fn a_group_data_sweep_stops_at_its_own_group() {
	let map = seeded_pair();
	let range = group_data_inner_range(GroupId::hashed(Hash128(9)));

	let strayed: Vec<GroupId> = map
		.encoded_range(OP, &range.start, &range.end, Scan::Forward, usize::MAX)
		.into_iter()
		.map(|(key, _)| OperatorStateKey::decode_inner(key.as_slice()).expect("a stored key decodes").0)
		.filter(|group| *group != GroupId::hashed(Hash128(9)))
		.collect();

	assert!(strayed.is_empty(), "a sweep of one group must never return another group's keys, got {strayed:?}");
}

type StagedRow = (GroupId, Vec<u8>, Option<Vec<u8>>);

fn stage_rows(bucket: &mut StandardBucket<JoinLeft>) -> Vec<StagedRow> {
	let mut rows = Vec::new();
	if let Some((frozen, _, _)) = bucket.freeze() {
		frozen.for_each(&mut |group, suffix, post| {
			rows.push((group, suffix.to_vec(), post.as_ref().map(|post| post.body().to_vec())));
		});
	}
	rows
}

fn staged_row(group: GroupId, n: u64, body: Option<&str>) -> StagedRow {
	(group, suffix(n).to_suffix_bytes().to_vec(), body.map(|body| body.as_bytes().to_vec()))
}

#[test]
fn a_group_settled_by_an_earlier_flush_is_staged_again_when_rewritten() {
	let seven = GroupId::hashed(Hash128(7));
	let nine = GroupId::hashed(Hash128(9));
	let mut bucket = bucket();
	bucket.record(seven, suffix(1), Some(row("first")));
	bucket.record(nine, suffix(2), Some(row("nine")));
	assert_eq!(stage_rows(&mut bucket).len(), 2);
	bucket.settle_flushing();

	bucket.record(seven, suffix(1), Some(row("second")));

	assert_eq!(
		stage_rows(&mut bucket),
		vec![staged_row(seven, 1, Some("second"))],
		"a rewrite of a settled row must be staged exactly once, and the untouched group must not be staged again"
	);
	assert_eq!(bucket.dirty_len(), 0);
	assert_eq!(bucket.dirty_footprint(), ByteSize::ZERO, "a staged group still charged would pin the flush budget");
}

#[test]
fn a_row_rewritten_mid_flush_is_left_dirty_by_settle_and_staged_by_the_next_flush() {
	let seven = GroupId::hashed(Hash128(7));
	let mut bucket = bucket();
	bucket.record(seven, suffix(1), Some(row("carried")));
	bucket.record(seven, suffix(2), Some(row("untouched")));
	assert_eq!(stage_rows(&mut bucket).len(), 2);

	bucket.record(seven, suffix(1), Some(row("rewritten")));
	bucket.settle_flushing();

	assert_eq!(bucket.staged_of(bucket.get(seven, &suffix(1)).expect("recorded")), Staged::Dirty);
	assert_eq!(bucket.staged_of(bucket.get(seven, &suffix(2)).expect("recorded")), Staged::Clean);
	assert_eq!(bucket.dirty_len(), 1);
	assert_eq!(
		stage_rows(&mut bucket),
		vec![staged_row(seven, 1, Some("rewritten"))],
		"the rewrite must reach the next flush, and the row the first flush persisted must not be written twice"
	);
}

#[test]
fn erasing_the_last_new_row_of_a_group_leaves_nothing_for_the_next_flush_to_find() {
	let seven = GroupId::hashed(Hash128(7));
	let nine = GroupId::hashed(Hash128(9));
	let mut bucket = bucket();
	bucket.record_fresh(seven, suffix(1), Some(row("fresh")));
	assert!(bucket.erase(seven, &suffix(1)));
	assert_eq!(bucket.dirty_len(), 0);
	assert_eq!(
		bucket.dirty_footprint(),
		ByteSize::ZERO,
		"an erased group still charged would hold flush budget for a group that no longer exists"
	);

	bucket.record(nine, suffix(2), Some(row("nine")));

	assert_eq!(stage_rows(&mut bucket), vec![staged_row(nine, 2, Some("nine"))]);
}

#[test]
fn a_flush_stages_groups_and_keys_in_ascending_order() {
	let groups: Vec<GroupId> = [9u128, 2, 5].into_iter().map(|n| GroupId::hashed(Hash128(n))).collect();
	let mut bucket = bucket();
	bucket.record(groups[0], suffix(3), None);
	bucket.record(groups[1], suffix(4), Some(row("b4")));
	bucket.record(groups[0], suffix(1), Some(row("a1")));
	bucket.record(groups[2], suffix(2), None);
	bucket.record(groups[1], suffix(1), None);
	bucket.record(groups[2], suffix(6), Some(row("c6")));
	bucket.record(groups[0], suffix(7), Some(row("a7")));

	let mut ascending = groups.clone();
	ascending.sort();
	let mut expected = Vec::new();
	for group in ascending {
		match group {
			g if g == groups[0] => expected.extend([
				staged_row(g, 1, Some("a1")),
				staged_row(g, 3, None),
				staged_row(g, 7, Some("a7")),
			]),
			g if g == groups[1] => expected.extend([staged_row(g, 1, None), staged_row(g, 4, Some("b4"))]),
			g => expected.extend([staged_row(g, 2, None), staged_row(g, 6, Some("c6"))]),
		}
	}

	assert_eq!(stage_rows(&mut bucket), expected);
}

#[test]
fn a_frozen_row_is_never_evicted_before_it_settles() {
	let seven = GroupId::hashed(Hash128(7));
	let mut bucket = bucket();
	bucket.record(seven, suffix(1), Some(row("frozen")));
	assert_eq!(stage_rows(&mut bucket).len(), 1);

	for _ in 0..2 {
		let (mut bytes, mut tombstones) = (ByteSize::from_bytes(u64::MAX), usize::MAX);
		let (evicted, _) = bucket.evict_clean(&mut bytes, &mut tombstones);
		assert_eq!(evicted, 0, "a row still on its way to the device was evicted");
	}
	assert_eq!(bucket.staged_of(bucket.get(seven, &suffix(1)).expect("recorded")), Staged::Flushing);

	bucket.settle_flushing();
	assert_eq!(bucket.staged_of(bucket.get(seven, &suffix(1)).expect("recorded")), Staged::Clean);
	let mut evicted = 0;
	for _ in 0..2 {
		let (mut bytes, mut tombstones) = (ByteSize::from_bytes(u64::MAX), usize::MAX);
		evicted += bucket.evict_clean(&mut bytes, &mut tombstones).0;
	}
	assert_eq!(evicted, 1, "a settled row must become evictable, otherwise the buffer never shrinks");
}

#[test]
fn a_sweep_stops_evicting_the_moment_its_deficit_is_paid() {
	let seven = GroupId::hashed(Hash128(7));
	let mut bucket = bucket();
	for n in 0..10 {
		bucket.record(seven, suffix(n), Some(row("settled")));
	}
	assert_eq!(stage_rows(&mut bucket).len(), 10);
	bucket.settle_flushing();

	let (mut bytes, mut tombstones) = (ByteSize::from_bytes(u64::MAX), 0usize);
	assert_eq!(bucket.evict_clean(&mut bytes, &mut tombstones).0, 0, "a fresh row earns one second chance");

	let (mut bytes, mut tombstones) = (ByteSize::from_bytes(1), 0usize);
	let (evicted, _) = bucket.evict_clean(&mut bytes, &mut tombstones);
	assert_eq!(evicted, 1, "one row pays a one byte deficit, so a second eviction is a sweep that did not stop");
	assert_eq!(bytes.as_bytes(), 0, "the deficit handed back must show it was paid");
	assert_eq!(bucket.len(), 9, "the rows past the stop must stay resident");
	assert!(bucket.get(seven, &suffix(0)).is_none(), "eviction walks in key order, so the first key goes first");
	assert!(bucket.get(seven, &suffix(9)).is_some(), "the last key lies past the stop");
}

#[test]
fn a_tombstone_deficit_evicts_tombstones_and_leaves_live_rows_alone() {
	let seven = GroupId::hashed(Hash128(7));
	let mut bucket = bucket();
	for n in 0..4 {
		bucket.record(seven, suffix(n), Some(row("live")));
	}
	assert_eq!(stage_rows(&mut bucket).len(), 4);
	bucket.settle_flushing();
	for n in 4..8 {
		bucket.record(seven, suffix(n), Some(row("doomed")));
	}
	assert_eq!(stage_rows(&mut bucket).len(), 4);
	bucket.settle_flushing();
	for n in 4..8 {
		bucket.record(seven, suffix(n), None);
	}
	assert_eq!(stage_rows(&mut bucket).len(), 4);
	bucket.settle_flushing();

	let mut evicted = 0;
	for _ in 0..2 {
		let (mut bytes, mut tombstones) = (ByteSize::ZERO, 2usize);
		evicted += bucket.evict_clean(&mut bytes, &mut tombstones).0;
	}
	assert_eq!(evicted, 2, "the sweep must free exactly the two tombstones it was asked for");
	for n in 0..4 {
		assert!(bucket.get(seven, &suffix(n)).is_some(), "live row {n} is not part of a tombstone deficit");
	}
}

#[test]
fn deleting_a_never_persisted_row_during_its_flush_leaves_a_tombstone() {
	let seven = GroupId::hashed(Hash128(7));
	let mut bucket = bucket();
	bucket.record_fresh(seven, suffix(1), Some(row("fresh")));
	assert_eq!(stage_rows(&mut bucket).len(), 1);

	assert!(!bucket.erase(seven, &suffix(1)), "a frozen row must not be erased as if it never existed");
	bucket.record(seven, suffix(1), None);
	assert!(!bucket.erase(seven, &suffix(1)), "the tombstone over a frozen row must not be collapsible either");
	bucket.settle_flushing();

	assert_eq!(stage_rows(&mut bucket), vec![staged_row(seven, 1, None)]);
}

#[test]
fn a_fresh_row_rewritten_before_its_first_flush_stays_collapsible() {
	let seven = GroupId::hashed(Hash128(7));
	let mut bucket = bucket();
	bucket.record_fresh(seven, suffix(1), Some(row("first")));
	bucket.record(seven, suffix(1), Some(row("second")));
	assert_eq!(bucket.staged_of(bucket.get(seven, &suffix(1)).expect("recorded")), Staged::Never);

	assert!(bucket.erase(seven, &suffix(1)));
	assert_eq!(bucket.dirty_len(), 0, "an erased row must leave nothing for the next flush to write");
	assert_eq!(bucket.dirty_footprint(), ByteSize::ZERO, "an erased row must stop charging the flush budget");
	assert!(stage_rows(&mut bucket).is_empty());
}

#[test]
fn only_tombstones_still_frozen_at_settle_count_as_clean() {
	let seven = GroupId::hashed(Hash128(7));
	let mut bucket = bucket();
	bucket.record(seven, suffix(1), None);
	bucket.record(seven, suffix(2), None);
	bucket.record(seven, suffix(3), None);
	assert_eq!(stage_rows(&mut bucket).len(), 3);
	assert_eq!(bucket.tombstone_len(), 0, "a tombstone not yet on the device must not count as clean");

	bucket.record(seven, suffix(1), Some(row("revived")));
	bucket.record(seven, suffix(2), None);
	bucket.settle_flushing();
	assert_eq!(bucket.tombstone_len(), 1, "only the tombstone left untouched during the flush is clean");

	assert_eq!(stage_rows(&mut bucket).len(), 2);
	bucket.settle_flushing();
	assert_eq!(bucket.tombstone_len(), 2, "the rewritten tombstone is clean once its own flush settles");
}

#[test]
fn a_tombstone_replaced_before_the_flush_is_not_counted_at_settle() {
	let seven = GroupId::hashed(Hash128(7));
	let mut bucket = bucket();
	bucket.record(seven, suffix(1), None);
	bucket.record(seven, suffix(1), Some(row("back")));
	bucket.record(seven, suffix(2), Some(row("alive")));
	bucket.record(seven, suffix(2), None);
	bucket.record(seven, suffix(2), None);
	assert_eq!(stage_rows(&mut bucket).len(), 2);
	bucket.settle_flushing();

	assert_eq!(bucket.tombstone_len(), 1);
}
