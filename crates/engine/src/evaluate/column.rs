// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::error::diagnostic::query::column_not_found;
use reifydb_core::expression::ColumnExpression;
use reifydb_core::frame::{ColumnValues, FrameColumn};
use reifydb_core::value::{Blob, Uuid4, Uuid7};
use reifydb_core::{Date, DateTime, Interval, RowId, Time, Value, error};

impl Evaluator {
    pub(crate) fn column(
        &mut self,
        column: &ColumnExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let name = column.0.fragment.to_string();

        // First try exact qualified name match
        if let Some(col) = ctx.columns.iter().find(|c| c.qualified_name() == name) {
            return self.extract_column_values(col, ctx);
        }

        // Then find all matches by unqualified name and select the most qualified one
        let all_matches: Vec<_> = ctx.columns.iter().filter(|c| c.name() == name).collect();

        if all_matches.is_empty() {
            return Err(error!(column_not_found(column.0.clone())));
        }

        // Always prefer the most qualified column available
        let best_match = all_matches
            .iter()
            .enumerate()
            .max_by_key(|(idx, c)| {
                let qualification_level = match (c.schema(), c.table()) {
                    (Some(_), Some(_)) => 3, // Fully qualified
                    (None, Some(_)) => 2,    // Table qualified
                    (Some(_), None) => 1,    // Schema qualified (unusual)
                    _ => 0,                  // Unqualified
                };
                // Use index as secondary sort key to prefer later columns in case of tie
                (qualification_level, *idx)
            })
            .map(|(_, c)| *c)
            .unwrap(); // Safe because we know the list is not empty

        self.extract_column_values(best_match, ctx)
    }

    fn extract_column_values(
        &mut self,
        col: &FrameColumn,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let take = ctx.take.unwrap_or(usize::MAX);

        match col.values().get_value(0) {
            Value::Bool(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Bool(b) => {
                            values.push(b);
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(false);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::bool_with_bitvec(values, bitvec)))
            }

            Value::Float4(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Float4(v) => {
                            values.push(v.value());
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(0.0f32);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::float4_with_bitvec(values, bitvec)))
            }

            Value::Float8(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Float8(v) => {
                            values.push(v.value());
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(0.0f64);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::float8_with_bitvec(values, bitvec)))
            }

            Value::Int1(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Int1(n) => {
                            values.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::int1_with_bitvec(values, bitvec)))
            }

            Value::Int2(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Int2(n) => {
                            values.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::int2_with_bitvec(values, bitvec)))
            }

            Value::Int4(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Int4(n) => {
                            values.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::int4_with_bitvec(values, bitvec)))
            }

            Value::Int8(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Int8(n) => {
                            values.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::int8_with_bitvec(values, bitvec)))
            }

            Value::Int16(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Int16(n) => {
                            values.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::int16_with_bitvec(values, bitvec)))
            }

            Value::Utf8(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Utf8(s) => {
                            values.push(s.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            values.push("".to_string());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::utf8_with_bitvec(values, bitvec)))
            }

            Value::Uint1(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uint1(n) => {
                            values.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::uint1_with_bitvec(values, bitvec)))
            }

            Value::Uint2(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uint2(n) => {
                            values.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::uint2_with_bitvec(values, bitvec)))
            }

            Value::Uint4(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uint4(n) => {
                            values.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::uint4_with_bitvec(values, bitvec)))
            }

            Value::Uint8(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uint8(n) => {
                            values.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::uint8_with_bitvec(values, bitvec)))
            }

            Value::Uint16(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uint16(n) => {
                            values.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::uint16_with_bitvec(values, bitvec)))
            }

            Value::Date(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Date(d) => {
                            values.push(d.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(Date::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::date_with_bitvec(values, bitvec)))
            }

            Value::DateTime(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::DateTime(dt) => {
                            values.push(dt.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(DateTime::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::datetime_with_bitvec(values, bitvec)))
            }

            Value::Time(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Time(t) => {
                            values.push(t.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(Time::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::time_with_bitvec(values, bitvec)))
            }

            Value::Interval(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Interval(i) => {
                            values.push(i.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(Interval::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::interval_with_bitvec(values, bitvec)))
            }
            Value::RowId(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::RowId(i) => {
                            values.push(i.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(RowId::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::row_id_with_bitvec(values, bitvec)))
            }
            Value::Uuid4(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uuid4(i) => {
                            values.push(i.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(Uuid4::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::uuid4_with_bitvec(values, bitvec)))
            }
            Value::Uuid7(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uuid7(i) => {
                            values.push(i.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(Uuid7::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::uuid7_with_bitvec(values, bitvec)))
            }
            Value::Blob(_) => {
                let mut values = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.values().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Blob(b) => {
                            values.push(b.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            values.push(Blob::new(vec![]));
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_values(ColumnValues::blob_with_bitvec(values, bitvec)))
            }
            Value::Undefined => {
                let count = std::cmp::min(ctx.row_count, take);
                Ok(col.with_new_values(ColumnValues::undefined(count)))
            }
        }
    }
}
