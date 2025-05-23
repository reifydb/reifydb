// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::aggregate::Aggregate;
use crate::{Column, ColumnValues, DataFrame};
use base::Value;

impl DataFrame {
    pub fn aggregate(&mut self, keys: &[&str], aggregates: &[Aggregate]) -> crate::Result<()> {
        let groups = self.group_by_view(keys)?;

        let mut key_columns: Vec<(String, Vec<Value>, Vec<bool>)> =
            keys.iter().map(|&k| (k.to_string(), Vec::new(), Vec::new())).collect();

        let mut agg_columns: Vec<(String, Vec<Value>, Vec<bool>)> =
            aggregates.iter().map(|agg| (agg.display_name(), Vec::new(), Vec::new())).collect();

        for (group_key, indices) in groups.iter() {
            // Populate key column values
            for (i, val) in group_key.0.iter().enumerate() {
                key_columns[i].1.push(val.clone());
                key_columns[i].2.push(!matches!(val, Value::Undefined));
            }

            // Populate aggregation results
            for (j, agg) in aggregates.iter().enumerate() {
                let value = agg.evaluate(self, &indices)?;
                agg_columns[j].1.push(value.clone());
                agg_columns[j].2.push(!matches!(value, Value::Undefined));
            }
        }

        let mut new_columns = Vec::with_capacity(key_columns.len() + agg_columns.len());

        for (name, values, valid) in key_columns.into_iter().chain(agg_columns) {
            let col = Column { name, data: ColumnValues::from_values(values, valid) };
            new_columns.push(col);
        }

        self.columns = new_columns;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn test() {
        todo!()
    }
}
