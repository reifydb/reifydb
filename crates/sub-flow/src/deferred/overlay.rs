// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::VecDeque, sync::Arc};

use reifydb_core::{actors::pending::Pending, common::CommitVersion};

pub struct FlowWriteOverlay {
	generations: VecDeque<(CommitVersion, Pending)>,
	merged: Arc<Pending>,
}

impl FlowWriteOverlay {
	pub fn new() -> Self {
		Self {
			generations: VecDeque::new(),
			merged: Arc::new(Pending::new()),
		}
	}

	pub fn promote(&mut self, version: CommitVersion, pending: Pending) {
		if pending.is_empty() {
			return;
		}
		Arc::make_mut(&mut self.merged).extend_from(&pending);
		self.generations.push_back((version, pending));
	}

	pub fn prune_through(&mut self, version: CommitVersion) {
		let mut pruned = false;
		while self.generations.front().is_some_and(|(v, _)| *v <= version) {
			self.generations.pop_front();
			pruned = true;
		}
		if !pruned {
			return;
		}
		let mut merged = Pending::new();
		for (_, pending) in self.generations.iter() {
			merged.extend_from(pending);
		}
		self.merged = Arc::new(merged);
	}

	pub fn merged(&self) -> Arc<Pending> {
		Arc::clone(&self.merged)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
	use reifydb_value::util::cowvec::CowVec;

	use super::*;

	fn key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	fn row(s: &str) -> EncodedRow {
		EncodedRow(CowVec::new(s.as_bytes().to_vec()))
	}

	fn pending_set(entries: &[(&str, &str)]) -> Pending {
		let mut p = Pending::new();
		for (k, v) in entries {
			p.insert(key(k), row(v));
		}
		p
	}

	#[test]
	fn promote_merges_generations_newest_wins() {
		let mut overlay = FlowWriteOverlay::new();
		overlay.promote(CommitVersion(10), pending_set(&[("a", "v10"), ("b", "v10")]));
		overlay.promote(CommitVersion(12), pending_set(&[("a", "v12")]));

		let merged = overlay.merged();
		assert_eq!(merged.get(&key("a")), Some(&row("v12")));
		assert_eq!(merged.get(&key("b")), Some(&row("v10")));
	}

	#[test]
	fn promote_skips_empty() {
		let mut overlay = FlowWriteOverlay::new();
		overlay.promote(CommitVersion(10), Pending::new());
		assert!(overlay.merged().is_empty());
		overlay.prune_through(CommitVersion(100));
		assert!(overlay.merged().is_empty());
	}

	#[test]
	fn promote_carries_tombstones() {
		let mut overlay = FlowWriteOverlay::new();
		overlay.promote(CommitVersion(10), pending_set(&[("a", "v10")]));
		let mut removal = Pending::new();
		removal.remove(key("a"));
		overlay.promote(CommitVersion(12), removal);

		let merged = overlay.merged();
		assert!(merged.is_removed(&key("a")));
		assert_eq!(merged.get(&key("a")), None);
	}

	#[test]
	fn prune_through_is_inclusive() {
		let mut overlay = FlowWriteOverlay::new();
		overlay.promote(CommitVersion(10), pending_set(&[("a", "v10")]));
		overlay.promote(CommitVersion(12), pending_set(&[("b", "v12")]));

		overlay.prune_through(CommitVersion(10));
		let merged = overlay.merged();
		assert_eq!(merged.get(&key("a")), None);
		assert_eq!(merged.get(&key("b")), Some(&row("v12")));

		overlay.prune_through(CommitVersion(12));
		assert!(overlay.merged().is_empty());
	}

	#[test]
	fn prune_below_front_keeps_everything() {
		let mut overlay = FlowWriteOverlay::new();
		overlay.promote(CommitVersion(10), pending_set(&[("a", "v10")]));
		let before = overlay.merged();

		overlay.prune_through(CommitVersion(9));
		let after = overlay.merged();
		assert!(Arc::ptr_eq(&before, &after));
		assert_eq!(after.get(&key("a")), Some(&row("v10")));
	}

	#[test]
	fn merged_survives_shared_reader_during_promote() {
		let mut overlay = FlowWriteOverlay::new();
		overlay.promote(CommitVersion(10), pending_set(&[("a", "v10")]));
		let reader = overlay.merged();

		overlay.promote(CommitVersion(12), pending_set(&[("a", "v12")]));

		assert_eq!(reader.get(&key("a")), Some(&row("v10")));
		assert_eq!(overlay.merged().get(&key("a")), Some(&row("v12")));
	}
}
