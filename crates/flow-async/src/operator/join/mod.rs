// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod column;
pub(crate) mod expiry;
pub mod operator;
pub(crate) mod snapshot;
pub mod state;
pub mod store;
pub mod strategy;

use arrow_array::RecordBatch;
use reifydb_core::{interface::change::Diff, value::batch::empty_batch};
use reifydb_value::value::row_number::RowNumber;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Identity<'a> {
	Mint,
	Existing,
	Consume,
	Carried(&'a [(RowNumber, bool)]),
}

pub(crate) struct Emitted {
	pub(crate) fresh: RecordBatch,
	pub(crate) existing: RecordBatch,
}

impl Emitted {
	pub(crate) fn empty() -> Self {
		Self {
			fresh: empty_batch(),
			existing: empty_batch(),
		}
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.fresh.num_rows() == 0 && self.existing.num_rows() == 0
	}

	pub(crate) fn published(self) -> Vec<Diff> {
		let mut out = Vec::new();
		if self.fresh.num_rows() > 0 {
			out.push(Diff::insert(self.fresh));
		}
		if self.existing.num_rows() > 0 {
			out.push(Diff::update(self.existing.clone(), self.existing));
		}
		out
	}

	pub(crate) fn withdrawn(self) -> Option<Diff> {
		(self.existing.num_rows() > 0).then(|| Diff::remove(self.existing))
	}
}
