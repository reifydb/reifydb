// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! What take owes its consumer: the `limit` live rows that arrived most recently, carrying their
//! current content.
//!
//! "Most recently" is an arrival order the operator assigns itself, not anything the row carries, and
//! it is the part worth being careful about. A row keeps the sequence it was first admitted under, so
//! an update does NOT make a row newer - which is why `Model::update` is overridden here rather than
//! left to default to retract-then-admit. An update split into a remove and an insert, which the
//! driver does deliberately, DOES make it newer, because the operator genuinely sees a fresh
//! admission. Those two must not be conflated or the oracle stops describing the operator.
//!
//! The oracle sorts a map and truncates. The operator maintains two indexes, evicts the lowest
//! sequence when it overflows, parks the evicted row in a bounded candidate buffer, and promotes the
//! highest-sequence candidate back when a slot frees. Neither derivation is the other.
//!
//! ## Why the corpus is bounded
//!
//! The candidate buffer is capped at `limit * 4` and prunes its oldest entries beyond that. A pruned
//! row can never be promoted again, so once pruning fires the view is a function of the eviction
//! history rather than of the live set, and "the newest `limit` live rows" stops being true. Since
//! candidates hold exactly the live rows outside the retained set, pruning is unreachable while
//! `live <= limit * 5`. The sweeps stay under that and `admit` asserts it, so widening a corpus past
//! the point where this oracle is valid fails here saying so, rather than as a mystery divergence.

use std::{cmp::Reverse, collections::BTreeMap};

use reifydb_testing_chaos::operator::{expectation::KeyedMultiset, model::Model, view::RowKey};
use reifydb_value::value::{Value, row_number::RowNumber};

use crate::operators::take::workload::{IDENTITY_COLUMN, TakeRow};

pub struct TakeOracle {
	limit: usize,
	next_seq: u64,
	live: BTreeMap<RowNumber, (u64, TakeRow)>,
}

impl TakeOracle {
	pub fn new(limit: usize) -> Self {
		Self {
			limit,
			next_seq: 0,
			live: BTreeMap::new(),
		}
	}

	fn retained(&self) -> Vec<&TakeRow> {
		let mut ordered: Vec<(u64, &TakeRow)> = self.live.values().map(|(seq, row)| (*seq, row)).collect();
		ordered.sort_by_key(|(seq, _)| Reverse(*seq));
		ordered.truncate(self.limit);
		ordered.into_iter().map(|(_, row)| row).collect()
	}

	fn claim(&self) -> KeyedMultiset {
		let mut rows: Vec<Vec<Value>> = self
			.retained()
			.into_iter()
			.map(|row| vec![Value::Int4(row.identity()), Value::Int8(row.value)])
			.collect();
		rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));

		// Keyed on the identity column so two output rows claiming the same source row are reported as
		// a collision rather than silently satisfying the multiset.
		KeyedMultiset::new(RowKey::columns([IDENTITY_COLUMN]), rows)
	}
}

impl Model<TakeRow> for TakeOracle {
	type Expectation = KeyedMultiset;

	fn admit(&mut self, row: &TakeRow) -> bool {
		let seq = self.next_seq;
		self.next_seq += 1;
		self.live.insert(row.number, (seq, row.clone()));

		if self.limit > 0 {
			assert!(
				self.live.len() <= self.limit * 5,
				"this corpus holds {} live rows against a limit of {}, which overflows the operator's \
				 candidate buffer (capped at limit * 4) and starts pruning rows that can then never be \
				 promoted back. Past that point the view is a function of eviction history rather than \
				 of the live set, and this oracle no longer describes it. Lower max_live to at most \
				 limit * 5, or drive the lossy regime with an invariant test instead of an exact one",
				self.live.len(),
				self.limit
			);
		}
		true
	}

	fn retract(&mut self, row: &TakeRow) {
		match self.live.remove(&row.number) {
			Some((_, held)) => assert_eq!(
				held.value, row.value,
				"the driver retracts the value it last admitted for row {:?}; a mismatch means the \
				 oracle and the corpus have diverged",
				row.number
			),
			None => panic!(
				"the driver retracted row {:?}, which the oracle never admitted",
				row.number
			),
		}
	}

	fn update(&mut self, pre: &TakeRow, post: &TakeRow) {
		// Overridden precisely because the default is retract-then-admit, which would hand the row a
		// fresh arrival sequence. The operator rewrites content in place and leaves the sequence alone,
		// so an updated row does not jump the queue ahead of rows that arrived after it.
		assert_eq!(pre.number, post.number, "an update must not change a row's number");
		let (seq, held) = self
			.live
			.remove(&pre.number)
			.unwrap_or_else(|| panic!("the driver updated row {:?}, which the oracle never admitted", pre.number));
		assert_eq!(
			held.value, pre.value,
			"the driver updates from the value it last admitted for row {:?}",
			pre.number
		);
		self.live.insert(post.number, (seq, post.clone()));
	}

	fn advance_ledger(&mut self, _at_ms: u64) {
		// Take holds no clock and nothing in flight; a tick moves nothing.
	}

	fn live(&self) -> KeyedMultiset {
		self.claim()
	}

	fn all(&self) -> KeyedMultiset {
		self.claim()
	}

	fn after_drain(&self) -> KeyedMultiset {
		self.claim()
	}
}
