// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_cdc::storage::{CdcStorage, memory::MemoryCdcStorage};
use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, SystemChange},
};
use reifydb_value::{util::cowvec::CowVec, value::datetime::DateTime};

fn cdc_at(version: u64) -> Cdc {
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(1_700_000_000_000_000_000),
		vec![SystemChange::Insert {
			key: EncodedKey::new(vec![1]),
			post: EncodedBytes(CowVec::new(vec![2])),
		}],
	)
}

#[test]
fn drop_before_advances_the_truncation_floor() {
	// The floor is max(deleted version) + 1, and a later drop at a lower cutoff must not move it
	// back: a consumer inside the truncated range would then pass the overtaken check and skip the gap.
	let storage = MemoryCdcStorage::new();
	for v in 1..=5u64 {
		storage.write(&cdc_at(v)).unwrap();
	}
	assert_eq!(storage.truncated_before().unwrap(), CommitVersion(0), "no truncation ran yet");

	storage.drop_before(CommitVersion(3), usize::MAX).unwrap();
	assert_eq!(storage.truncated_before().unwrap(), CommitVersion(3), "1 and 2 deleted, floor = 2 + 1");

	storage.drop_before(CommitVersion(2), usize::MAX).unwrap();
	assert_eq!(storage.truncated_before().unwrap(), CommitVersion(3), "the floor never moves backwards");
}

#[test]
fn a_limited_drop_only_claims_what_it_actually_removed() {
	// A limited drop stops early, so the floor may only advance past what was deleted; claiming the
	// full cutoff while rows below it survive would resync consumers that could still read them.
	let storage = MemoryCdcStorage::new();
	for v in 1..=5u64 {
		storage.write(&cdc_at(v)).unwrap();
	}

	storage.drop_before(CommitVersion(5), 2).unwrap();
	assert_eq!(
		storage.truncated_before().unwrap(),
		CommitVersion(3),
		"versions 1 and 2 were deleted, so the floor is 2 + 1"
	);
	assert_eq!(storage.min_version().unwrap(), Some(CommitVersion(3)));
}

#[test]
fn a_drop_that_deletes_nothing_never_advances_the_floor() {
	// The TTL task routinely drops at a cutoff below the oldest stored version, and CDC versions are
	// sparse, so "absent below the cutoff" does not mean "deleted". Advancing the floor here would
	// declare a fresh consumer overtaken over versions that never existed.
	let storage = MemoryCdcStorage::new();
	for v in 5..=8u64 {
		storage.write(&cdc_at(v)).unwrap();
	}

	storage.drop_before(CommitVersion(4), usize::MAX).unwrap();
	assert_eq!(
		storage.truncated_before().unwrap(),
		CommitVersion(0),
		"nothing was deleted, so no consumer can have missed anything"
	);
	assert_eq!(storage.len(), 4);
}
