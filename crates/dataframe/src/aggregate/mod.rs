// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{ColumnValues, DataFrame, Error};
use base::Value;

pub enum Aggregate {
    Sum(String),
    Count(String),
    Min(String),
    Max(String),
    Avg(String),
}

impl Aggregate {
    pub fn evaluate(&self, df: &DataFrame, indices: &[usize]) -> crate::Result<Value> {
        let col = |name: &str| {
            df.columns
                .iter()
                .find(|c| c.name == name)
                .ok_or_else(|| Error(format!("column '{}' not found", name)))
        };

        match self {
            Aggregate::Avg(col_name) => match &col(col_name)?.data {
                ColumnValues::Int2(vals, valid) => {
                    let (sum, count): (i32, usize) = indices
                        .iter()
                        .filter(|&&i| valid[i])
                        .map(|&i| (vals[i] as i32, 1))
                        .fold((0, 0), |(a, b), (v, c)| (a + v, b + c));

                    if count > 0 {
                        Ok(Value::float8(sum as f64 / count as f64))
                    } else {
                        Ok(Value::Undefined)
                    }
                }
                _ => Err("AVG only supports Int2 columns".into()),
            },

            Aggregate::Sum(col_name) => match &col(col_name.as_str())?.data {
                ColumnValues::Int2(vals, valid) => Ok(Value::Int2(
                    indices.iter().filter(|&&i| valid[i]).map(|&i| vals[i] as i32).sum::<i32>()
                        as i16,
                )),
                _ => Err("SUM only supports Int2".into()),
            },
            Aggregate::Count(col_name) => {
                if col_name == &"*" {
                    Ok(Value::Int2(indices.len() as i16))
                } else {
                    match &col(col_name)?.data {
                        ColumnValues::Float8(_, valid)
                        | ColumnValues::Int2(_, valid)
                        | ColumnValues::Bool(_, valid)
                        | ColumnValues::Text(_, valid) => {
                            Ok(Value::Int2(indices.iter().filter(|&&i| valid[i]).count() as i16))
                        }
                        ColumnValues::Undefined(_) => Ok(Value::Int2(0)),
                    }
                }
            }
            Aggregate::Min(col_name) => match &col(col_name)?.data {
                ColumnValues::Int2(vals, valid) => Ok(Value::Int2(
                    indices.iter().filter(|&&i| valid[i]).map(|&i| vals[i]).min().unwrap_or(0),
                )),
                _ => Err("MIN only supports Int2".into()),
            },
            Aggregate::Max(col_name) => match &col(col_name)?.data {
                ColumnValues::Int2(vals, valid) => Ok(Value::Int2(
                    indices.iter().filter(|&&i| valid[i]).map(|&i| vals[i]).max().unwrap_or(0),
                )),
                _ => Err("MAX only supports Int2".into()),
            },
        }
    }

    pub fn display_name(&self) -> String {
        match self {
            Aggregate::Sum(c) => format!("sum({})", c),
            Aggregate::Count(c) => format!("count({})", c),
            Aggregate::Min(c) => format!("min({})", c),
            Aggregate::Max(c) => format!("max({})", c),
            Aggregate::Avg(c) => format!("avg({})", c),
        }
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
