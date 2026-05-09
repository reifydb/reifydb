// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::row::Row;
use reifydb_type::value::row_number::RowNumber;

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
