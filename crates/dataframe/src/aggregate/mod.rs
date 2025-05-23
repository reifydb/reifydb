// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{ColumnValues, DataFrame, Error};

pub enum Aggregate {
    Sum(String),
    Count(String),
    Min(String),
    Max(String),
    Avg(String),
}

impl Aggregate {
    pub fn evaluate(&self, df: &DataFrame, indices: &[usize]) -> crate::Result<ColumnValues> {
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

                    let (v, is_valid) =
                        if count > 0 { ((sum as f64 / count as f64), true) } else { (0.0, false) };

                    Ok(ColumnValues::Float8(vec![v], vec![is_valid]))
                }
                _ => Err("AVG only supports Int2 columns".into()),
            },

            Aggregate::Sum(col_name) => match &col(col_name)?.data {
                ColumnValues::Int2(vals, valid) => {
                    let sum: i32 =
                        indices.iter().filter(|&&i| valid[i]).map(|&i| vals[i] as i32).sum();
                    Ok(ColumnValues::Int2(vec![sum as i16], vec![true]))
                }
                _ => Err("SUM only supports Int2".into()),
            },

            Aggregate::Count(col_name) => {
                let count = if col_name == "*" {
                    indices.len()
                } else {
                    match &col(col_name)?.data {
                        ColumnValues::Float8(_, valid)
                        | ColumnValues::Int2(_, valid)
                        | ColumnValues::Bool(_, valid)
                        | ColumnValues::Text(_, valid) => {
                            indices.iter().filter(|&&i| valid[i]).count()
                        }
                        ColumnValues::Undefined(_) => 0,
                    }
                };
                Ok(ColumnValues::Int2(vec![count as i16], vec![true]))
            }

            Aggregate::Min(col_name) => match &col(col_name)?.data {
                ColumnValues::Int2(vals, valid) => {
                    let min = indices.iter().filter(|&&i| valid[i]).map(|&i| vals[i]).min();
                    match min {
                        Some(v) => Ok(ColumnValues::Int2(vec![v], vec![true])),
                        None => Ok(ColumnValues::Int2(vec![0], vec![false])),
                    }
                }
                _ => Err("MIN only supports Int2".into()),
            },

            Aggregate::Max(col_name) => match &col(col_name)?.data {
                ColumnValues::Int2(vals, valid) => {
                    let max = indices.iter().filter(|&&i| valid[i]).map(|&i| vals[i]).max();
                    match max {
                        Some(v) => Ok(ColumnValues::Int2(vec![v], vec![true])),
                        None => Ok(ColumnValues::Int2(vec![0], vec![false])),
                    }
                }
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
