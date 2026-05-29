// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::slice::Iter;

use reifydb_core::row::Row;
use reifydb_value::value::row_number::RowNumber;

#[derive(Debug, Clone)]
pub enum ChaosEvent {
	Insert {
		row_number: RowNumber,
		row: Row,
	},
	Update {
		row_number: RowNumber,
		pre: Row,
		post: Row,
	},
	Remove {
		row_number: RowNumber,
		row: Row,
	},
}

impl ChaosEvent {
	pub fn row_number(&self) -> RowNumber {
		match self {
			ChaosEvent::Insert {
				row_number,
				..
			}
			| ChaosEvent::Update {
				row_number,
				..
			}
			| ChaosEvent::Remove {
				row_number,
				..
			} => *row_number,
		}
	}

	pub fn is_insert(&self) -> bool {
		matches!(self, ChaosEvent::Insert { .. })
	}

	pub fn is_update(&self) -> bool {
		matches!(self, ChaosEvent::Update { .. })
	}

	pub fn is_remove(&self) -> bool {
		matches!(self, ChaosEvent::Remove { .. })
	}
}

#[derive(Debug, Clone)]
pub struct ChaosBatch {
	pub events: Vec<ChaosEvent>,
}

impl ChaosBatch {
	pub fn new(events: Vec<ChaosEvent>) -> Self {
		Self {
			events,
		}
	}

	pub fn iter(&self) -> Iter<'_, ChaosEvent> {
		self.events.iter()
	}

	pub fn len(&self) -> usize {
		self.events.len()
	}

	pub fn is_empty(&self) -> bool {
		self.events.is_empty()
	}
}

impl<'a> IntoIterator for &'a ChaosBatch {
	type Item = &'a ChaosEvent;
	type IntoIter = std::slice::Iter<'a, ChaosEvent>;

	fn into_iter(self) -> Self::IntoIter {
		self.events.iter()
	}
}
