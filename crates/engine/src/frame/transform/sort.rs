// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Error, Frame};
use reifydb_core::{SortDirection, SortKey};

impl Frame {
    pub fn sort(&mut self, keys: &[SortKey]) -> crate::frame::Result<()> {
        let row_count = self.columns.first().map_or(0, |c| c.data.len());

        // 1. Create index indirection (0..n)
        let mut indices: Vec<usize> = (0..row_count).collect();

        // 2. Resolve column references and sorting directions
        let key_refs: Vec<(&ColumnValues, &SortDirection)> = keys
            .iter()
            .map(|key| {
                let col = self
                    .columns
                    .iter()
                    .find(|c| c.name == key.column)
                    .ok_or_else(|| format!("Column '{}' not found", key.column))?;
                Ok::<_, Error>((&col.data, &key.direction))
            })
            .collect::<Result<_, _>>()?;

        // 3. Sort the indices using comparator
        indices.sort_unstable_by(|&a, &b| {
            for (col, dir) in &key_refs {
                let va = col.get(a);
                let vb = col.get(b);
                let ord = va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal);
                let ord = match dir {
                    SortDirection::Asc => ord,
                    SortDirection::Desc => ord.reverse(),
                };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });

        // 4. Reorder all columns in place using the sorted index
        for col in &mut self.columns {
            col.data.reorder(&indices);
        }

        Ok(())
    }
}
