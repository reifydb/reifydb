// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Column, Frame};

impl Frame {
    pub fn project<F>(&mut self, f: F) -> crate::Result<()>
    where
        F: FnOnce(&[&Column], usize) -> Result<Vec<Column>, Box<dyn std::error::Error>>,
    {
        let row_count = self.columns.first().map_or(0, |col| col.data.len());

        let columns: Vec<&Column> = self.columns.iter().map(|c| c).collect();
        self.columns = f(&columns, row_count)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn implement() {
        todo!()
    }
}
