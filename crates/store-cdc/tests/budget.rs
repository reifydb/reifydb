// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::Bound;

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::cdc::{Cdc, CdcChange},
};
use reifydb_store_cdc::{storage::CdcStorage, types::cdc_resident_bytes};
use reifydb_value::{util::cowvec::CowVec, value::datetime::DateTime};

mod common;
use common::Fixture;

const RECORDS: u64 = 60;
const UNFLUSHED: u64 = 7;

fn record(version: u64) -> Cdc {
	let payload = 64 * (1 + (version * 7 % 5) as usize);
	Cdc::new(
		ChangeVersion::from(CommitVersion(version)),
		DateTime::from_nanos(version as i64),
		vec![CdcChange::Insert {
			key: EncodedKey::new(version.to_be_bytes().to_vec()),
			post: EncodedBytes(CowVec::new(vec![0xab; payload])),
		}],
	)
}

fn size(cdc: &Cdc) -> u64 {
	cdc_resident_bytes(cdc).as_bytes()
}

fn filled(fixture: Fixture) -> Fixture {
	for version in 1..=RECORDS {
		fixture.store.write(&record(version)).unwrap();
		if version % 10 == 0 && version <= RECORDS - UNFLUSHED {
			assert!(fixture.store.flush_pending(), "flush must not time out");
		}
	}
	fixture
}

fn fixtures() -> Vec<(&'static str, Fixture)> {
	vec![
		("memory", filled(common::memory())),
		("memory_cached", filled(common::memory_cached())),
		("sqlite", filled(common::sqlite())),
		("sqlite_cached", filled(common::sqlite_cached())),
		("sqlite_starved", filled(common::sqlite_starved_cache())),
	]
}

fn versions(items: &[Cdc]) -> Vec<u64> {
	items.iter().map(|cdc| cdc.version.commit.0).collect()
}

#[test]
fn budgeted_pieces_read_exactly_the_sequence_one_unbudgeted_read_returns() {
	// a budget stop that loses or repeats a version makes a flow skip or double-apply a change.
	for (name, fixture) in fixtures() {
		let whole =
			versions(&fixture.store.read_range(Bound::Unbounded, Bound::Unbounded, 10_000).unwrap().items);
		assert_eq!(whole.len() as u64, RECORDS, "{name}: precondition, every record is readable");
		for budget in [1, 300, 700, 2_000, 1 << 30] {
			let mut start = Bound::Unbounded;
			let mut pieced = Vec::new();
			loop {
				let batch = fixture
					.store
					.read_range_within(start, Bound::Unbounded, 256, budget, size)
					.unwrap();
				assert!(
					!batch.items.is_empty(),
					"{name} budget {budget}: a read with more to give returned nothing"
				);
				let bytes: u64 = batch.items.iter().map(size).sum();
				assert!(
					batch.items.len() == 1 || bytes <= budget,
					"{name} budget {budget}: {} items took {bytes} bytes, over the budget",
					batch.items.len()
				);
				pieced.extend(versions(&batch.items));
				start = Bound::Excluded(batch.items.last().unwrap().version.commit);
				if !batch.has_more {
					break;
				}
			}
			assert_eq!(
				pieced, whole,
				"{name} budget {budget}: pieces must join into the unbudgeted sequence"
			);
		}
	}
}

#[test]
fn a_budget_smaller_than_one_entry_still_returns_that_entry_alone() {
	// returning nothing for an entry larger than the budget would stall the reader on it forever.
	for (name, fixture) in fixtures() {
		let batch = fixture.store.read_range_within(Bound::Unbounded, Bound::Unbounded, 256, 1, size).unwrap();

		assert_eq!(versions(&batch.items), vec![1], "{name}: exactly the first entry must come back");
		assert!(batch.has_more, "{name}: a budget stop inside the range must report more to read");
	}
}
