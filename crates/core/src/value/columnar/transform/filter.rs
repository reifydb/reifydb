// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{BitVec, value::columnar::Columns};

impl<'a> Columns<'a> {
	pub fn filter(&mut self, mask: &BitVec) -> crate::Result<()> {
		let columns = self.0.make_mut();
		for column in columns.iter_mut() {
			column.filter(mask)?;
		}
		Ok(())
	}
}
