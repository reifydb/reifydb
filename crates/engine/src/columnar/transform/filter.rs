// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::columns::Columns;
use reifydb_core::BitVec;

impl Columns {
    pub fn filter(&mut self, mask: &BitVec) -> crate::Result<()> {
        for column in self.iter_mut() {
            column.filter(mask)?;
        }
        Ok(())
    }
}
