// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::shape::RowFamily,
};
use reifydb_core::{
	interface::store::{EntryKind, MultiVersionRow},
	state::horizon::Cutoff,
};
use reifydb_store_multi::tier::persistent::MultiPersistentTier;
use reifydb_transaction::{multi::RangeScope, transaction::command::CommandTransaction};
use reifydb_value::{Result, value::datetime::DateTime};

pub struct ExpiredScan {
	pub expired: Vec<MultiVersionRow>,
	pub min_survivor_row: Option<u64>,
	pub next_cursor: Option<EncodedKey>,
}

pub type ExpiryCursor = (DateTime, EncodedKey);

pub struct ExpiredIndexScan {
	pub expired: Vec<MultiVersionRow>,
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
		let Some(row) = txn.get(key)? else {
			continue;
		};
		if family.updated_at(&row.bytes) <= cutoff.instant() {
			expired.push(row);
		}
	}

	Ok(ExpiredIndexScan {
		expired,
		next_cursor,
	})
}

pub fn keyspace_start(range: &EncodedKeyRange) -> EncodedKey {
	match &range.start {
		Bound::Included(key) | Bound::Excluded(key) => key.clone(),
		Bound::Unbounded => EncodedKey::new(Vec::new()),
	}
}

pub fn resume_range(base: &EncodedKeyRange, cursor: Option<&EncodedKey>) -> EncodedKeyRange {
	match cursor {
		Some(key) => EncodedKeyRange::new(base.start.clone(), Bound::Excluded(key.clone())),
		None => base.clone(),
	}
}

pub fn scan_expired(
	txn: &mut CommandTransaction,
	range: EncodedKeyRange,
	family: RowFamily,
	cutoff: Cutoff,
	limit: usize,
	row_number_of: &dyn Fn(&EncodedKey) -> Option<u64>,
) -> Result<ExpiredScan> {
	let mut expired: Vec<MultiVersionRow> = Vec::new();
	let mut min_survivor_row: Option<u64> = None;
	let mut next_cursor: Option<EncodedKey> = None;

	if limit == 0 {
		return Ok(ExpiredScan {
			expired,
			min_survivor_row,
			next_cursor,
		});
	}

	let mut examined = 0usize;
	let mut current: Option<MultiVersionRow> = None;

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
	row: MultiVersionRow,
	family: RowFamily,
	cutoff: Cutoff,
	row_number_of: &dyn Fn(&EncodedKey) -> Option<u64>,
	expired: &mut Vec<MultiVersionRow>,
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
