// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use reifydb_value::Result;

use crate::value::column::columns::Columns;

impl Columns {
	pub fn filter(&mut self, mask: &BooleanBuffer) -> Result<()> {
		self.system.filter(mask);

		let columns = &mut self.columns;
		for column in columns.iter_mut() {
			column.filter(mask)?;
		}
		Ok(())
	}
}
