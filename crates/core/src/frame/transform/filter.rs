// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::Frame;
use crate::BitVec;

impl Frame {
    pub fn filter(&mut self, mask: &BitVec) -> crate::Result<()> {
        for column in self.columns.iter_mut() {
            column.filter(mask)?;
        }
        Ok(())
    }
}
