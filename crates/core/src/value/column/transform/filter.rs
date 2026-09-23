// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_buffer::BooleanBuffer;
use arrow_select::filter::FilterPredicate;
use reifydb_value::{Result, util::kernel};

use crate::value::column::columns::Columns;

impl Columns {
	pub fn filter(&mut self, mask: &BooleanBuffer) -> Result<()> {
		self.system.filter(mask);

		let columns = &mut self.columns;
		let mut shared: Option<(usize, FilterPredicate)> = None;
		for column in columns.iter_mut() {
			let len = column.len();
			let predicate = match &shared {
				Some((cached_len, predicate)) if *cached_len == len => predicate,
				_ => &shared.insert((len, kernel::shared_predicate(mask, len))).1,
			};
			column.filter_with(predicate)?;
		}
		Ok(())
	}
}
