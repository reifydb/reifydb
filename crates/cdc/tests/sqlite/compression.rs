// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Roundtrip tests proving that CDC blocks compressed at any supported zstd
//! level decode back to the exact same entries, and that mixed-level blocks
//! coexist transparently in the same store.

use std::collections::Bound;

use reifydb_cdc::{
	compact::block,
	storage::{CdcStorage, sqlite::storage::SqliteCdcStorage},
};
use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::cdc::{Cdc, SystemChange},
};
use reifydb_type::{util::cowvec::CowVec, value::datetime::DateTime};

const TEST_LEVELS: &[u8] = &[1, 3, 7, 19, 22];

fn cdc_diverse(version: u64) -> Cdc {
	let n_changes = ((version % 3) + 1) as usize;
	let changes: Vec<SystemChange> = (0..n_changes)
		.map(|i| SystemChange::Insert {
			key: EncodedKey::new(format!("k-{}-{}", version, i).into_bytes()),
			post: EncodedRow(CowVec::new(
				format!("v-{}-{}-{}", version, i, "x".repeat((version as usize) % 17)).into_bytes(),
			)),
		})
		.collect();
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(1_700_000_000_000_000_000 + version * 1_000_000),
		Vec::new(),
		changes,
	)
}

fn cdc_bytes(cdc: &Cdc) -> Vec<u8> {
	postcard::to_stdvec(cdc).unwrap()
}

fn cdcs_bytes(cdcs: &[Cdc]) -> Vec<Vec<u8>> {
	cdcs.iter().map(cdc_bytes).collect()
}

#[test]
fn block_encode_decode_roundtrip_across_all_levels() {
	let entries: Vec<Cdc> = (1..=200u64).map(cdc_diverse).collect();
	let original = cdcs_bytes(&entries);

	let mut payload_sizes: Vec<usize> = Vec::new();
	for &level in TEST_LEVELS {
		let payload = block::encode(&entries, level).unwrap();
		let decoded = block::decode(&payload).unwrap();
		assert_eq!(cdcs_bytes(&decoded), original, "level {level} decode diverged");
		payload_sizes.push(payload.len());
	}

	assert!(
		payload_sizes.windows(2).any(|w| w[0] != w[1]),
		"expected at least two levels to produce different payload sizes; got {payload_sizes:?}",
	);
}

#[test]
fn compact_all_roundtrip_per_level() {
	for &level in TEST_LEVELS {
		let store = SqliteCdcStorage::in_memory();
		let entries: Vec<Cdc> = (1..=1024u64).map(cdc_diverse).collect();
		for cdc in &entries {
			store.write(cdc).unwrap();
		}

		let summaries = store.compact_all(256, level, CommitVersion(u64::MAX)).unwrap();
		assert_eq!(summaries.len(), 4, "level {level}: expected 4 blocks");
		assert_eq!(summaries.iter().map(|s| s.num_entries).sum::<usize>(), 1024);

		let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, 2048).unwrap();
		assert!(!batch.has_more, "level {level}: unexpected has_more");
		assert_eq!(batch.items.len(), 1024, "level {level}: count mismatch");
		assert_eq!(cdcs_bytes(&batch.items), cdcs_bytes(&entries), "level {level}: range readback diverged");

		for v in [1u64, 256, 257, 512, 1024] {
			let got = store.read(CommitVersion(v)).unwrap().expect("entry");
			assert_eq!(cdc_bytes(&got), cdc_bytes(&entries[(v - 1) as usize]), "level {level} v{v}");
		}
	}
}

#[test]
fn read_back_mixed_compression_levels() {
	let store = SqliteCdcStorage::in_memory();
	let all_entries: Vec<Cdc> = (1..=1024u64).map(cdc_diverse).collect();

	for cdc in &all_entries[..512] {
		store.write(cdc).unwrap();
	}
	let s1 = store.compact_all(512, 1, CommitVersion(u64::MAX)).unwrap();
	assert_eq!(s1.len(), 1, "first batch should produce exactly one block");

	for cdc in &all_entries[512..] {
		store.write(cdc).unwrap();
	}
	let s2 = store.compact_all(512, 22, CommitVersion(u64::MAX)).unwrap();
	assert_eq!(s2.len(), 1, "second batch should produce exactly one block");

	let batch = store.read_range(Bound::Unbounded, Bound::Unbounded, 2048).unwrap();
	assert!(!batch.has_more);
	assert_eq!(batch.items.len(), 1024);
	assert_eq!(cdcs_bytes(&batch.items), cdcs_bytes(&all_entries));

	let level1_pick = store.read(CommitVersion(100)).unwrap().expect("v100");
	let level22_pick = store.read(CommitVersion(900)).unwrap().expect("v900");
	assert_eq!(cdc_bytes(&level1_pick), cdc_bytes(&all_entries[99]));
	assert_eq!(cdc_bytes(&level22_pick), cdc_bytes(&all_entries[899]));
}
