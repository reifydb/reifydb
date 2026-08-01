// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod column;
pub mod operator;
pub(crate) mod snapshot;
pub mod state;
pub mod store;
pub mod strategy;

use reifydb_core::{interface::change::Diff, value::column::columns::Columns};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Identity {
	Mint,
	Existing,
	Consume,
}

pub(crate) struct Emitted {
	pub(crate) fresh: Columns,
	pub(crate) existing: Columns,
}

impl Emitted {
	pub(crate) fn empty() -> Self {
		Self {
			fresh: Columns::empty(),
			existing: Columns::empty(),
		}
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.fresh.is_empty() && self.existing.is_empty()
	}

	pub(crate) fn published(self) -> Vec<Diff> {
		let mut out = Vec::new();
		if !self.fresh.is_empty() {
			out.push(Diff::insert(self.fresh));
		}
		if !self.existing.is_empty() {
			out.push(Diff::update(self.existing.clone(), self.existing));
		}
		out
	}

	pub(crate) fn withdrawn(self) -> Option<Diff> {
		(!self.existing.is_empty()).then(|| Diff::remove(self.existing))
	}
}
