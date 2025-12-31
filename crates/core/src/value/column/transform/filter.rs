// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use crate::{BitVec, util::CowVec, value::column::Columns};

impl Columns {
	pub fn filter(&mut self, mask: &BitVec) -> crate::Result<()> {
		// Filter encoded numbers if present
		if !self.row_numbers.is_empty() {
			let filtered_row_numbers: Vec<_> = self
				.row_numbers
				.iter()
				.enumerate()
				.filter(|(i, _)| *i < mask.len() && mask.get(*i))
				.map(|(_, &row_num)| row_num)
				.collect();
			self.row_numbers = CowVec::new(filtered_row_numbers);
		}

		// Filter columns
		let columns = self.columns.make_mut();
		for column in columns.iter_mut() {
			column.filter(mask)?;
		}
		Ok(())
	}
}
