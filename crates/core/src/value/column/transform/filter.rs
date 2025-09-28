// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{BitVec, util::CowVec, value::column::Columns};

impl<'a> Columns<'a> {
	pub fn filter(&mut self, mask: &BitVec) -> crate::Result<()> {
		// Filter row numbers if present
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
