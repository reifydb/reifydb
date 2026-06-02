// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{cmp::Ordering, iter::Peekable, vec::IntoIter};

use reifydb_core::{
	actors::pending::PendingWrite, common::CommitVersion, encoded::key::EncodedKey,
	interface::store::MultiVersionRow,
};
use reifydb_value::Result;

pub(crate) struct FlowMergePendingIterator<I>
where
	I: Iterator<Item = Result<MultiVersionRow>>,
{
	storage_iter: Peekable<I>,
	pending_iter: Peekable<IntoIter<(EncodedKey, PendingWrite)>>,
	version: CommitVersion,
	forward: bool,
}

impl<I> Iterator for FlowMergePendingIterator<I>
where
	I: Iterator<Item = Result<MultiVersionRow>>,
{
	type Item = Result<MultiVersionRow>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let next_storage = self.storage_iter.peek();

			match (self.pending_iter.peek(), next_storage) {
				(Some((pending_key, _)), Some(storage_result)) => {
					let storage_val = match storage_result {
						Ok(v) => v,
						Err(_) => {
							let err = self.storage_iter.next().unwrap();
							return Some(err);
						}
					};
					let cmp = pending_key.cmp(&storage_val.key);

					let pending_leads = if self.forward {
						matches!(cmp, Ordering::Less)
					} else {
						matches!(cmp, Ordering::Greater)
					};

					if pending_leads {
						let (key, value) = self.pending_iter.next().unwrap();
						if let PendingWrite::Set(row) = value {
							return Some(Ok(MultiVersionRow {
								key,
								row,
								version: self.version,
							}));
						}
					} else if matches!(cmp, Ordering::Equal) {
						let (key, value) = self.pending_iter.next().unwrap();
						self.storage_iter.next();
						if let PendingWrite::Set(row) = value {
							return Some(Ok(MultiVersionRow {
								key,
								row,
								version: self.version,
							}));
						}
					} else {
						return Some(self.storage_iter.next().unwrap());
					}
				}
				(Some(_), None) => {
					let (key, value) = self.pending_iter.next().unwrap();
					if let PendingWrite::Set(row) = value {
						return Some(Ok(MultiVersionRow {
							key,
							row,
							version: self.version,
						}));
					}
				}
				(None, Some(_)) => {
					return Some(self.storage_iter.next().unwrap());
				}
				(None, None) => return None,
			}
		}
	}
}

pub(crate) fn flow_merge_pending_iterator<I>(
	pending: Vec<(EncodedKey, PendingWrite)>,
	storage_iter: I,
	version: CommitVersion,
) -> FlowMergePendingIterator<I>
where
	I: Iterator<Item = Result<MultiVersionRow>>,
{
	FlowMergePendingIterator {
		storage_iter: storage_iter.peekable(),
		pending_iter: pending.into_iter().peekable(),
		version,
		forward: true,
	}
}

pub(crate) fn flow_merge_pending_iterator_rev<I>(
	pending: Vec<(EncodedKey, PendingWrite)>,
	storage_iter: I,
	version: CommitVersion,
) -> FlowMergePendingIterator<I>
where
	I: Iterator<Item = Result<MultiVersionRow>>,
{
	FlowMergePendingIterator {
		storage_iter: storage_iter.peekable(),
		pending_iter: pending.into_iter().peekable(),
		version,
		forward: false,
	}
}
