// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{Result, util::bitvec::BitVec};

use crate::value::column::columns::Columns;

impl Columns {
	pub fn filter(&mut self, mask: &BitVec) -> Result<()> {
		self.system.filter(mask);

		let columns = &mut self.columns;
		for column in columns.iter_mut() {
			column.filter(mask)?;
		}
		Ok(())
	}
}
