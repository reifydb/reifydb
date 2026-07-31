// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_cdc::storage::{CdcStorage, memory::MemoryCdcStorage};
use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, SystemChange},
};
use reifydb_value::{util::cowvec::CowVec, value::datetime::DateTime};

fn cdc_at(version: u64) -> Cdc {
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(1_700_000_000_000_000_000),
		Vec::new(),
		vec![SystemChange::Insert {
			key: EncodedKey::new(vec![1]),
			post: EncodedRow(CowVec::new(vec![2])),
		}],
	)
}

#[test]
fn drop_before_advances_the_truncation_floor() {
	// The floor is max(deleted version) + 1: a consumer at or past it missed nothing. A later
	// no-op drop at a LOWER cutoff must not move it backwards, or a consumer sitting in the
	// already-truncated range would pass the overtaken check and silently skip the gap.
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
	// With a batch limit the drop stops early. The floor may only advance past what was DELETED,
	// not to the requested cutoff: claiming the full cutoff while rows below it still exist
	// would resync consumers that could in fact still read everything they need.
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
	// The root cause of the subscription parity SIGABRTs: the TTL task routinely calls
	// drop_before with a cutoff at or below the oldest stored version (nothing expired yet),
	// and cdc versions are sparse, so "absent below the cutoff" does NOT mean "deleted". If a
	// no-op drop advanced the floor, a fresh consumer whose cursor sits at the default seed
	// would be declared overtaken over versions that never existed and be resynced or
	// terminated for no reason.
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
