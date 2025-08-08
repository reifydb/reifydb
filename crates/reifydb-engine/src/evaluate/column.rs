// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::{Column, ColumnData};
use crate::evaluate::{EvaluationContext, Evaluator};
use reifydb_core::result::error::diagnostic::query::column_not_found;
use reifydb_core::value::{Blob, Uuid4, Uuid7};
use reifydb_core::{Date, DateTime, Interval, RowId, Time, Value, error};
use reifydb_rql::expression::ColumnExpression;

impl Evaluator {
    pub(crate) fn column(
        &mut self,
        column: &ColumnExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<Column> {
        let name = column.0.fragment.to_string();

        // First try exact qualified name match
        if let Some(col) = ctx.columns.iter().find(|c| c.qualified_name() == name) {
            return self.extract_column_data(col, ctx);
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

        self.extract_column_data(best_match, ctx)
    }

    fn extract_column_data(
        &mut self,
        col: &Column,
        ctx: &EvaluationContext,
    ) -> crate::Result<Column> {
        let take = ctx.take.unwrap_or(usize::MAX);

        match col.data().get_value(0) {
            Value::Bool(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Bool(b) => {
                            data.push(b);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(false);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::bool_with_bitvec(data, bitvec)))
            }

            Value::Float4(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Float4(v) => {
                            data.push(v.value());
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0.0f32);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::float4_with_bitvec(data, bitvec)))
            }

            Value::Float8(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Float8(v) => {
                            data.push(v.value());
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0.0f64);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::float8_with_bitvec(data, bitvec)))
            }

            Value::Int1(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Int1(n) => {
                            data.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::int1_with_bitvec(data, bitvec)))
            }

            Value::Int2(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Int2(n) => {
                            data.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::int2_with_bitvec(data, bitvec)))
            }

            Value::Int4(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Int4(n) => {
                            data.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::int4_with_bitvec(data, bitvec)))
            }

            Value::Int8(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Int8(n) => {
                            data.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::int8_with_bitvec(data, bitvec)))
            }

            Value::Int16(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Int16(n) => {
                            data.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::int16_with_bitvec(data, bitvec)))
            }

            Value::Utf8(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Utf8(s) => {
                            data.push(s.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            data.push("".to_string());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::utf8_with_bitvec(data, bitvec)))
            }

            Value::Uint1(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uint1(n) => {
                            data.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::uint1_with_bitvec(data, bitvec)))
            }

            Value::Uint2(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uint2(n) => {
                            data.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::uint2_with_bitvec(data, bitvec)))
            }

            Value::Uint4(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uint4(n) => {
                            data.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::uint4_with_bitvec(data, bitvec)))
            }

            Value::Uint8(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uint8(n) => {
                            data.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::uint8_with_bitvec(data, bitvec)))
            }

            Value::Uint16(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uint16(n) => {
                            data.push(n);
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(0);
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::uint16_with_bitvec(data, bitvec)))
            }

            Value::Date(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Date(d) => {
                            data.push(d.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(Date::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::date_with_bitvec(data, bitvec)))
            }

            Value::DateTime(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::DateTime(dt) => {
                            data.push(dt.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(DateTime::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::datetime_with_bitvec(data, bitvec)))
            }

            Value::Time(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Time(t) => {
                            data.push(t.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(Time::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::time_with_bitvec(data, bitvec)))
            }

            Value::Interval(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Interval(i) => {
                            data.push(i.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(Interval::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::interval_with_bitvec(data, bitvec)))
            }
            Value::RowId(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::RowId(i) => {
                            data.push(i.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(RowId::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::row_id_with_bitvec(data, bitvec)))
            }
            Value::Uuid4(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uuid4(i) => {
                            data.push(i.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(Uuid4::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::uuid4_with_bitvec(data, bitvec)))
            }
            Value::Uuid7(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Uuid7(i) => {
                            data.push(i.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(Uuid7::default());
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::uuid7_with_bitvec(data, bitvec)))
            }
            Value::Blob(_) => {
                let mut data = Vec::new();
                let mut bitvec = Vec::new();
                let mut count = 0;
                for v in col.data().iter() {
                    if count >= take {
                        break;
                    }
                    match v {
                        Value::Blob(b) => {
                            data.push(b.clone());
                            bitvec.push(true);
                        }
                        _ => {
                            data.push(Blob::new(vec![]));
                            bitvec.push(false);
                        }
                    }
                    count += 1;
                }
                Ok(col.with_new_data(ColumnData::blob_with_bitvec(data, bitvec)))
            }
            Value::Undefined => {
                let count = std::cmp::min(ctx.row_count, take);
                Ok(col.with_new_data(ColumnData::undefined(count)))
            }
        }
    }
}
