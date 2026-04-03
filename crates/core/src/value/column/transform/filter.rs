// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::{
	Result,
	util::{bitvec::BitVec, cowvec::CowVec},
};

use crate::value::column::columns::Columns;

impl Columns {
	pub fn filter(&mut self, mask: &BitVec) -> Result<()> {
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

		// Filter created_at timestamps if present
		if !self.created_at.is_empty() {
			let filtered_created_at: Vec<_> = self
				.created_at
				.iter()
				.enumerate()
				.filter(|(i, _)| *i < mask.len() && mask.get(*i))
				.map(|(_, &ts)| ts)
				.collect();
			self.created_at = CowVec::new(filtered_created_at);
		}

		// Filter updated_at timestamps if present
		if !self.updated_at.is_empty() {
			let filtered_updated_at: Vec<_> = self
				.updated_at
				.iter()
				.enumerate()
				.filter(|(i, _)| *i < mask.len() && mask.get(*i))
				.map(|(_, &ts)| ts)
				.collect();
			self.updated_at = CowVec::new(filtered_updated_at);
		}

		// Filter columns
		let columns = self.columns.make_mut();
		for column in columns.iter_mut() {
			column.filter(mask)?;
		}
		Ok(())
	}
}
