// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeSet;

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	interface::{catalog::id::QueueId, store::SingleVersionRangeRev},
	key::{EncodableKey, queue_schedule::QueueKeyActiveKey},
};
use reifydb_value::{Result, util::cowvec::CowVec, value::row_number::RowNumber};

use crate::single::{SingleTransaction, write::SingleWriteTransaction};

#[derive(Debug, Default)]
pub struct ChainOverlay {
	added: BTreeSet<(u64, RowNumber)>,
	removed: BTreeSet<(u64, RowNumber)>,
}

impl ChainOverlay {
	fn of_key(set: &BTreeSet<(u64, RowNumber)>, key_hash: u64) -> impl Iterator<Item = RowNumber> + '_ {
		set.range((key_hash, RowNumber(0))..=(key_hash, RowNumber(u64::MAX))).map(|(_, row)| *row)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainHead {
	Empty,
	Single(RowNumber),
	Multiple(RowNumber),
}

pub fn chain_peek(
	single: &SingleTransaction,
	overlay: &ChainOverlay,
	queue: QueueId,
	partition: u16,
	key_hash: u64,
) -> Result<ChainHead> {
	let store = single.read_store();
	let budget = 2 + ChainOverlay::of_key(&overlay.removed, key_hash).count() as u64;
	let batch = SingleVersionRangeRev::range_rev_batch(
		&store,
		QueueKeyActiveKey::key_scan(queue, partition, key_hash),
		budget,
	)?;

	let mut rows: BTreeSet<RowNumber> = batch
		.items
		.iter()
		.filter_map(|item| QueueKeyActiveKey::decode(&item.key))
		.map(|key| key.row)
		.filter(|row| !overlay.removed.contains(&(key_hash, *row)))
		.collect();
	rows.extend(ChainOverlay::of_key(&overlay.added, key_hash));

	let mut ascending = rows.into_iter();

	Ok(match (ascending.next(), ascending.next()) {
		(None, _) => ChainHead::Empty,
		(Some(row), None) => ChainHead::Single(row),
		(Some(row), Some(_)) => ChainHead::Multiple(row),
	})
}

pub fn chain_add(
	tx: &mut SingleWriteTransaction<'_>,
	overlay: &mut ChainOverlay,
	queue: QueueId,
	partition: u16,
	key_hash: u64,
	row: RowNumber,
) -> Result<()> {
	tx.set(&QueueKeyActiveKey::encoded(queue, partition, key_hash, row), EncodedBytes(CowVec::new(vec![])))?;
	overlay.removed.remove(&(key_hash, row));
	overlay.added.insert((key_hash, row));

	Ok(())
}

pub fn chain_remove(
	tx: &mut SingleWriteTransaction<'_>,
	overlay: &mut ChainOverlay,
	queue: QueueId,
	partition: u16,
	key_hash: u64,
	row: RowNumber,
) -> Result<()> {
	tx.remove(&QueueKeyActiveKey::encoded(queue, partition, key_hash, row))?;
	overlay.added.remove(&(key_hash, row));
	overlay.removed.insert((key_hash, row));

	Ok(())
}
