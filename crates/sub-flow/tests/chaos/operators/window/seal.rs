// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeSet;

use reifydb_value::value::row_number::RowNumber;

#[derive(Default)]
pub struct Seal {
	high: Option<(u64, u64)>,
	sealed_high: Option<(u64, u64)>,
	tail: BTreeSet<(u64, u64)>,
}

impl Seal {
	pub fn push(&mut self, coord_ms: u64, row: RowNumber, immutable_ms: u64) {
		let key = (coord_ms, row.0);
		// A sealed min never takes a row at or behind the newest row it froze.
		if self.sealed_high.is_some_and(|sealed| key <= sealed) {
			return;
		}
		let high = self.high.map_or(key, |high| high.max(key));
		self.high = Some(high);
		self.tail.insert(key);
		while let Some(&oldest) = self.tail.first()
			&& high.0 - oldest.0 > immutable_ms
		{
			self.sealed_high = Some(oldest);
			self.tail.pop_first();
		}
	}

	pub fn remove(&mut self, coord_ms: u64, row: RowNumber) {
		self.tail.remove(&(coord_ms, row.0));
	}

	pub fn holds_a_frozen_row(&self) -> bool {
		self.sealed_high.is_some()
	}
}
