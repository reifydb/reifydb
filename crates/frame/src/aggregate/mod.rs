// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{ColumnValues, Frame, Error};

pub enum Aggregate {
    Sum(String),
    Count(String),
    Min(String),
    Max(String),
    Avg(String),
}

impl Aggregate {
    pub fn evaluate(&self, df: &Frame, indices: &[usize]) -> crate::Result<ColumnValues> {
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

                    Ok(ColumnValues::float8_with_validity(vec![v], vec![is_valid]))
                }
                _ => Err("AVG only supports Int2 columns".into()),
            },

            Aggregate::Sum(col_name) => match &col(col_name)?.data {
                ColumnValues::Int2(vals, valid) => {
                    let sum: i32 =
                        indices.iter().filter(|&&i| valid[i]).map(|&i| vals[i] as i32).sum();
                    Ok(ColumnValues::int2(vec![sum as i16]))
                }
                _ => Err("SUM only supports Int2".into()),
            },

            Aggregate::Count(col_name) => {
                let count = if col_name == "*" {
                    indices.len()
                } else {
                    match &col(col_name)?.data {
                        ColumnValues::Bool(_, valid)
                        | ColumnValues::Float4(_, valid)
                        | ColumnValues::Float8(_, valid)
                        | ColumnValues::Int1(_, valid)
                        | ColumnValues::Int2(_, valid)
                        | ColumnValues::Int4(_, valid)
                        | ColumnValues::Int8(_, valid)
                        | ColumnValues::Int16(_, valid)
                        | ColumnValues::Uint1(_, valid)
                        | ColumnValues::Uint2(_, valid)
                        | ColumnValues::Uint4(_, valid)
                        | ColumnValues::Uint8(_, valid)
                        | ColumnValues::Uint16(_, valid)
                        | ColumnValues::String(_, valid) => {
                            indices.iter().filter(|&&i| valid[i]).count()
                        }
                        ColumnValues::Undefined(_) => 0,
                    }
                };
                Ok(ColumnValues::int2(vec![count as i16]))
            }
            Aggregate::Min(col_name) => match &col(col_name)?.data {
                ColumnValues::Int2(vals, valid) => {
                    let min = indices.iter().filter(|&&i| valid[i]).map(|&i| vals[i]).min();
                    match min {
                        Some(v) => Ok(ColumnValues::int2(vec![v])),
                        None => Ok(ColumnValues::int2_with_validity(vec![0], vec![false])),
                    }
                }
                _ => Err("MIN only supports Int2".into()),
            },

            Aggregate::Max(col_name) => match &col(col_name)?.data {
                ColumnValues::Int2(vals, valid) => {
                    let max = indices.iter().filter(|&&i| valid[i]).map(|&i| vals[i]).max();
                    match max {
                        Some(v) => Ok(ColumnValues::int2(vec![v])),
                        None => Ok(ColumnValues::int2_with_validity(vec![0], vec![false])),
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
