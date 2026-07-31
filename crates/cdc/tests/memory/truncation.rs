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
	// The floor claims "everything below this is gone". A full drop moves it to the requested
	// cutoff; a later no-op drop at a LOWER cutoff must not move it backwards, or a consumer
	// sitting in the already-truncated range would pass the overtaken check and silently skip
	// the gap.
	let storage = MemoryCdcStorage::new();
	for v in 1..=5u64 {
		storage.write(&cdc_at(v)).unwrap();
	}
	assert_eq!(storage.truncated_before().unwrap(), CommitVersion(0), "no truncation ran yet");

	storage.drop_before(CommitVersion(3), usize::MAX).unwrap();
	assert_eq!(storage.truncated_before().unwrap(), CommitVersion(3));

	storage.drop_before(CommitVersion(2), usize::MAX).unwrap();
	assert_eq!(storage.truncated_before().unwrap(), CommitVersion(3), "the floor never moves backwards");
}

#[test]
fn a_limited_drop_only_claims_what_it_contiguously_removed() {
	// With a batch limit the drop may stop early. The floor may only advance to the first
	// SURVIVING version, not the requested cutoff: claiming the full cutoff while rows below it
	// still exist would resync consumers that could in fact still read everything they need.
	let storage = MemoryCdcStorage::new();
	for v in 1..=5u64 {
		storage.write(&cdc_at(v)).unwrap();
	}

	storage.drop_before(CommitVersion(5), 2).unwrap();
	assert_eq!(
		storage.truncated_before().unwrap(),
		CommitVersion(3),
		"versions 1 and 2 are gone, 3 survives, so the floor is 3"
	);
	assert_eq!(storage.min_version().unwrap(), Some(CommitVersion(3)));
}
