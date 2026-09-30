// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Reverse,
	collections::{BinaryHeap, VecDeque},
	ops::Bound,
};

use reifydb_core::{
	interface::{catalog::storage::StorageId, store::MultiVersionRow},
	internal_err,
	key::{
		any::TaggedKey, bound::TaggedKeyBoundRange, row::PartitionedRowKey,
		series::PartitionedSeriesRowKeyRange,
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::value::partition::Partition;

use crate::Result;

#[derive(Clone, Copy)]
pub(crate) enum MergeLayout {
	Row,
	Series,
}

type MergeOrder = (Option<u8>, u64, u64);

impl MergeLayout {
	fn range(self, storage: StorageId, partition: Option<Partition>) -> TaggedKeyBoundRange {
		match (self, partition) {
			(Self::Row, Some(partition)) => PartitionedRowKey::partition_range(storage, partition),
			(Self::Row, None) => PartitionedRowKey::full_scan(storage),
			(Self::Series, Some(partition)) => {
				PartitionedSeriesRowKeyRange::partition_range(storage, partition)
			}
			(Self::Series, None) => PartitionedSeriesRowKeyRange::full_scan(storage),
		}
	}

	fn locate(self, key: &TaggedKey) -> Result<(Partition, MergeOrder)> {
		match (self, key) {
			(Self::Row, TaggedKey::PartitionedRow(key)) => Ok((key.partition, (None, key.row.0, 0))),
			(Self::Series, TaggedKey::PartitionedSeriesRow(key)) => {
				Ok((key.partition, (key.variant_tag, key.key, key.sequence)))
			}
			(_, key) => internal_err!("a partitioned oldest-first scan yielded a foreign key {:?}", key),
		}
	}
}

struct MergeHead {
	range: TaggedKeyBoundRange,
	last: Option<TaggedKey>,
	buffer: VecDeque<MultiVersionRow<TaggedKey>>,
	exhausted: bool,
}

impl MergeHead {
	fn over(range: TaggedKeyBoundRange) -> Self {
		Self {
			range,
			last: None,
			buffer: VecDeque::new(),
			exhausted: false,
		}
	}
}

pub(crate) struct PartitionMerge {
	layout: MergeLayout,
	storage: StorageId,
	fixed: Option<TaggedKeyBoundRange>,
	heads: Vec<MergeHead>,
	queue: BinaryHeap<Reverse<(MergeOrder, usize)>>,
	opened: bool,
}

impl PartitionMerge {
	pub(crate) fn new(layout: MergeLayout, storage: StorageId, fixed: Option<TaggedKeyBoundRange>) -> Self {
		Self {
			layout,
			storage,
			fixed,
			heads: Vec::new(),
			queue: BinaryHeap::new(),
			opened: false,
		}
	}

	pub(crate) fn next(
		&mut self,
		rx: &mut Transaction<'_>,
		batch_size: u64,
	) -> Result<Vec<MultiVersionRow<TaggedKey>>> {
		if !self.opened {
			self.open(rx, batch_size)?;
		}
		let mut rows = Vec::new();
		while (rows.len() as u64) < batch_size {
			let Some(Reverse((_, index))) = self.queue.pop() else {
				break;
			};
			let Some(row) = self.heads[index].buffer.pop_front() else {
				return internal_err!(
					"a queued partition head of an oldest-first scan had no buffered row"
				);
			};
			rows.push(row);
			self.enqueue(rx, index, batch_size)?;
		}
		Ok(rows)
	}

	fn open(&mut self, rx: &mut Transaction<'_>, batch_size: u64) -> Result<()> {
		self.opened = true;
		match self.fixed.take() {
			Some(range) => self.heads.push(MergeHead::over(range)),
			None => self.discover(rx)?,
		}
		for index in 0..self.heads.len() {
			self.enqueue(rx, index, batch_size)?;
		}
		Ok(())
	}

	fn discover(&mut self, rx: &mut Transaction<'_>) -> Result<()> {
		let outer = self.layout.range(self.storage, None);
		let mut end = outer.end.clone();
		loop {
			let remaining = TaggedKeyBoundRange {
				start: outer.start.clone(),
				end,
			};
			let Some(first) = rx.range_rev(remaining, RangeScope::All, 1)?.next().transpose()? else {
				return Ok(());
			};
			let (partition, _) = self.layout.locate(&first.key)?;
			let range = self.layout.range(self.storage, Some(partition));
			let Bound::Included(prefix) = range.start.clone() else {
				return internal_err!(
					"a partition range of an oldest-first scan has no inclusive prefix start"
				);
			};
			end = Bound::Excluded(prefix);
			self.heads.push(MergeHead::over(range));
		}
	}

	fn enqueue(&mut self, rx: &mut Transaction<'_>, index: usize, batch_size: u64) -> Result<()> {
		let head = &mut self.heads[index];
		if head.buffer.is_empty() && !head.exhausted {
			let range = head.range.clone().resume_before(head.last.as_ref());
			head.buffer = rx
				.range_rev(range, RangeScope::All, batch_size as usize)?
				.take(batch_size as usize)
				.collect::<Result<VecDeque<_>>>()?;
			head.exhausted = (head.buffer.len() as u64) < batch_size;
			if let Some(row) = head.buffer.back() {
				head.last = Some(row.key.clone());
			}
		}
		if let Some(row) = head.buffer.front() {
			let (_, order) = self.layout.locate(&row.key)?;
			self.queue.push(Reverse((order, index)));
		}
		Ok(())
	}
}
