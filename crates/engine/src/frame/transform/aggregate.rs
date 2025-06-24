// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::aggregate::Aggregate;
use crate::frame::{Column, ColumnValues, Frame};

impl Frame {
    pub fn aggregate(&mut self, keys: &[&str], aggregates: &[Aggregate]) -> crate::frame::Result<()> {
        let groups = self.group_by_view(keys)?;

        let mut key_columns: Vec<Column> = keys
            .iter()
            .map(|&k| Column { name: k.to_string(), data: ColumnValues::int2([]) })
            .collect();

        let mut aggregate_columns: Vec<Column> = aggregates
            .iter()
            .map(|agg| Column { name: agg.display_name(), data: ColumnValues::undefined(0) })
            .collect();

        for (group_key, indices) in groups {
            // Populate key column values
            for (i, value) in group_key.into_iter().enumerate() {
                let column = &mut key_columns.get_mut(i).unwrap().data;
                column.push_value(value);
            }

            // Populate aggregation results
            for (j, agg) in aggregates.iter().enumerate() {
                let column = aggregate_columns.get_mut(j).unwrap();
                column.data.extend(agg.evaluate(self, &indices)?)?;
            }
        }

        self.columns = key_columns.into_iter().chain(aggregate_columns).collect();

        Ok(())
    }
}
