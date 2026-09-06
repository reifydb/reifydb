// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, ops::Bound};

use reifydb_codec::{key::encoded::EncodedKey, row::shape::RowFamily};
use reifydb_core::{
	interface::store::{EntryKind, MultiVersionRow},
	key::{any::AnyKey, bound::AnyKeyBoundRange},
	state::horizon::Cutoff,
};
use reifydb_store_multi::tier::persistent::MultiPersistentTier;
use reifydb_transaction::{multi::RangeScope, transaction::command::CommandTransaction};
use reifydb_value::{Result, value::datetime::DateTime};

pub struct ExpiredScan {
	pub expired: Vec<MultiVersionRow<AnyKey>>,
	pub min_survivor_row: Option<u64>,
	pub next_cursor: Option<AnyKey>,
}

pub type ExpiryCursor = (DateTime, EncodedKey);

pub struct ExpiredIndexScan {
	pub expired: Vec<AnyKey>,
	pub next_cursor: Option<ExpiryCursor>,
}

pub fn scan_expired_indexed(
	txn: &mut CommandTransaction,
	persistent: &MultiPersistentTier,
	kind: EntryKind,
	family: RowFamily,
	cutoff: Cutoff,
	cursor: Option<&ExpiryCursor>,
	limit: usize,
) -> Result<ExpiredIndexScan> {
	let candidates = persistent.expired_keys(
		kind,
		cutoff.instant(),
		cursor.map(|(at, key)| (*at, key.as_slice())),
		limit,
	)?;
	let next_cursor = candidates.last().map(|(key, at)| (*at, key.clone()));

	let mut expired = Vec::with_capacity(candidates.len());
	for (key, _) in &candidates {
		let key = AnyKey::decode(key).expect("an expired index key must decode");
		let Some(row) = txn.get(&key)? else {
			continue;
		};
		if family.updated_at(&row.bytes) <= cutoff.instant() {
			expired.push(key);
		}
	}

	Ok(ExpiredIndexScan {
		expired,
		next_cursor,
	})
}

pub fn min_survivor_row(
	txn: &mut CommandTransaction,
	keyspace: AnyKeyBoundRange,
	deleted: &[AnyKey],
	row_number_of: &dyn Fn(&AnyKey) -> Option<u64>,
) -> Result<Option<u64>> {
	let skip: BTreeSet<&AnyKey> = deleted.iter().collect();
	let mut seen: Option<AnyKey> = None;

	let mut stream = txn.range_rev_persistence(keyspace, RangeScope::All, 1024)?;
	for entry in stream.by_ref() {
		let entry = entry?;
		if seen.as_ref() == Some(&entry.key) {
			continue;
		}
		seen = Some(entry.key.clone());
		if skip.contains(&entry.key) {
			continue;
		}
		if let Some(row_number) = row_number_of(&entry.key) {
			return Ok(Some(row_number));
		}
	}

	Ok(None)
}

pub fn keyspace_start(range: &AnyKeyBoundRange) -> EncodedKey {
	match &range.start {
		Bound::Included(bound) | Bound::Excluded(bound) => bound.encode(),
		Bound::Unbounded => EncodedKey::new(Vec::new()),
	}
}

pub fn resume_range(base: &AnyKeyBoundRange, cursor: Option<&AnyKey>) -> AnyKeyBoundRange {
	base.clone().resume_before(cursor)
}

pub fn scan_expired(
	txn: &mut CommandTransaction,
	range: AnyKeyBoundRange,
	family: RowFamily,
	cutoff: Cutoff,
	limit: usize,
	row_number_of: &dyn Fn(&AnyKey) -> Option<u64>,
) -> Result<ExpiredScan> {
	let mut expired: Vec<MultiVersionRow<AnyKey>> = Vec::new();
	let mut min_survivor_row: Option<u64> = None;
	let mut next_cursor: Option<AnyKey> = None;

	if limit == 0 {
		return Ok(ExpiredScan {
			expired,
			min_survivor_row,
			next_cursor,
		});
	}

	let mut examined = 0usize;
	let mut current: Option<MultiVersionRow<AnyKey>> = None;

	let mut stream = txn.range_rev_persistence(range, RangeScope::All, 1024)?;
	for entry in stream.by_ref() {
		let entry = entry?;
		if let Some(cur) = &mut current {
			if cur.key == entry.key {
				if entry.version > cur.version {
					*cur = entry;
				}
				continue;
			}
			let finished = current.take().unwrap();
			let finished_key = finished.key.clone();
			classify(finished, family, cutoff, row_number_of, &mut expired, &mut min_survivor_row);
			examined += 1;
			if examined >= limit {
				if let Some(row_number) = row_number_of(&entry.key) {
					min_survivor_row = fold_min(min_survivor_row, row_number);
				}
				next_cursor = Some(finished_key);
				break;
			}
		}
		current = Some(entry);
	}
	drop(stream);

	if let Some(finished) = current.take() {
		classify(finished, family, cutoff, row_number_of, &mut expired, &mut min_survivor_row);
	}

	Ok(ExpiredScan {
		expired,
		min_survivor_row,
		next_cursor,
	})
}

fn classify(
	row: MultiVersionRow<AnyKey>,
	family: RowFamily,
	cutoff: Cutoff,
	row_number_of: &dyn Fn(&AnyKey) -> Option<u64>,
	expired: &mut Vec<MultiVersionRow<AnyKey>>,
	min_survivor_row: &mut Option<u64>,
) {
	if family.updated_at(&row.bytes) <= cutoff.instant() {
		expired.push(row);
	} else if let Some(row_number) = row_number_of(&row.key) {
		*min_survivor_row = fold_min(*min_survivor_row, row_number);
	}
}

fn fold_min(current: Option<u64>, candidate: u64) -> Option<u64> {
	Some(current.map_or(candidate, |m| m.min(candidate)))
}
