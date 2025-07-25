// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::Type::Undefined;
use crate::Value;
use crate::frame::column::{ColumnQualified, TableQualified};
use crate::frame::iterator::FrameIter;
use crate::frame::{ColumnValues, FrameColumn};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Frame {
    pub name: String,
    pub columns: Vec<FrameColumn>,
    pub index: HashMap<String, usize>, // Maps qualified_name -> column index
    pub frame_index: HashMap<(String, String), usize>, // Maps (frame, name) -> index
}

impl Frame {
    pub fn single_row<'a>(rows: impl IntoIterator<Item = (&'a str, Value)>) -> Frame {
        let mut columns = Vec::new();
        let mut index = HashMap::new();

        for (idx, (name, value)) in rows.into_iter().enumerate() {
            let values = match value {
                Value::Undefined => ColumnValues::Undefined(1),
                Value::Bool(v) => ColumnValues::bool([v]),
                Value::Float4(v) => ColumnValues::float4([v.into()]),
                Value::Float8(v) => ColumnValues::float8([v.into()]),
                Value::Int1(v) => ColumnValues::int1([v]),
                Value::Int2(v) => ColumnValues::int2([v]),
                Value::Int4(v) => ColumnValues::int4([v]),
                Value::Int8(v) => ColumnValues::int8([v]),
                Value::Int16(v) => ColumnValues::int16([v]),
                Value::Utf8(ref v) => ColumnValues::utf8([v.clone()]),
                Value::Uint1(v) => ColumnValues::uint1([v]),
                Value::Uint2(v) => ColumnValues::uint2([v]),
                Value::Uint4(v) => ColumnValues::uint4([v]),
                Value::Uint8(v) => ColumnValues::uint8([v]),
                Value::Uint16(v) => ColumnValues::uint16([v]),
                Value::Date(ref v) => ColumnValues::date([v.clone()]),
                Value::DateTime(ref v) => ColumnValues::datetime([v.clone()]),
                Value::Time(ref v) => ColumnValues::time([v.clone()]),
                Value::Interval(ref v) => ColumnValues::interval([v.clone()]),
                Value::RowId(v) => ColumnValues::row_id([v]),
                Value::Uuid4(v) => ColumnValues::uuid4([v]),
                Value::Uuid7(v) => ColumnValues::uuid7([v]),
            };

            let column = FrameColumn::TableQualified(TableQualified {
                table: "frame".to_string(),
                name: name.to_string(),
                values,
            });
            index.insert(column.qualified_name(), idx);
            columns.push(column);
        }

        let (final_index, frame_index) = build_indices(&columns);
        Frame { name: "frame".to_string(), columns, index: final_index, frame_index }
    }
}

impl Frame {
    pub fn new(columns: Vec<FrameColumn>) -> Self {
        let n = columns.first().map_or(0, |c| c.values().len());
        assert!(columns.iter().all(|c| c.values().len() == n));

        let (index, frame_index) = build_indices(&columns);
        Self { name: "frame".to_string(), columns, index, frame_index }
    }

    pub fn new_with_name(columns: Vec<FrameColumn>, name: impl Into<String>) -> Self {
        let n = columns.first().map_or(0, |c| c.values().len());
        assert!(columns.iter().all(|c| c.values().len() == n));

        let (index, frame_index) = build_indices(&columns);
        Self { name: name.into(), columns, index, frame_index }
    }

    pub fn empty() -> Self {
        Self {
            name: "frame".to_string(),
            columns: vec![],
            index: HashMap::new(),
            frame_index: HashMap::new(),
        }
    }

    pub fn shape(&self) -> (usize, usize) {
        (self.columns.get(0).map(|c| c.values().len()).unwrap_or(0), self.columns.len())
    }

    pub fn is_empty(&self) -> bool {
        self.shape().0 == 0
    }

    pub fn row(&self, i: usize) -> Vec<Value> {
        self.columns.iter().map(|c| c.values().get(i)).collect()
    }

    pub fn column(&self, name: &str) -> Option<&FrameColumn> {
        // Try qualified name first, then try as original name
        self.index
            .get(name)
            .map(|&i| &self.columns[i])
            .or_else(|| self.columns.iter().find(|col| col.name() == name))
    }

    pub fn column_by_source(&self, frame: &str, name: &str) -> Option<&FrameColumn> {
        self.frame_index.get(&(frame.to_string(), name.to_string())).map(|&i| &self.columns[i])
    }

    pub fn column_values(&self, name: &str) -> Option<&ColumnValues> {
        // Try qualified name first, then try as original name
        self.index
            .get(name)
            .map(|&i| self.columns[i].values())
            .or_else(|| self.columns.iter().find(|col| col.name() == name).map(|col| col.values()))
    }

    pub fn column_values_mut(&mut self, name: &str) -> Option<&mut ColumnValues> {
        // Try qualified name first, then try as original name
        if let Some(&i) = self.index.get(name) {
            Some(self.columns[i].values_mut())
        } else {
            let pos = self.columns.iter().position(|col| col.name() == name)?;
            Some(self.columns[pos].values_mut())
        }
    }

    pub fn iter(&self) -> FrameIter<'_> {
        let col_names = self.columns.iter().map(|c| c.qualified_name()).collect::<Vec<_>>();
        FrameIter {
            df: self,
            row_index: 0,
            row_total: self.shape().0,
            column_index: Arc::new(col_names),
        }
    }

    pub fn row_count(&self) -> usize {
        self.columns.first().map_or(0, |col| col.values().len())
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_row(&self, index: usize) -> Vec<Value> {
        self.columns.iter().map(|col| col.values().get(index)).collect()
    }
}

impl Frame {
    pub fn from_rows(names: &[&str], result_rows: &[Vec<Value>]) -> Self {
        let column_count = names.len();

        let mut columns: Vec<FrameColumn> = names
            .iter()
            .map(|name| {
                FrameColumn::ColumnQualified(ColumnQualified {
                    name: name.to_string(),
                    values: ColumnValues::with_capacity(Undefined, 0),
                })
            })
            .collect();

        for row in result_rows {
            assert_eq!(row.len(), column_count, "row length does not match column count");
            for (i, value) in row.iter().enumerate() {
                columns[i].values_mut().push_value(value.clone());
            }
        }

        Frame::new(columns)
    }
}

fn build_indices(
    columns: &[FrameColumn],
) -> (HashMap<String, usize>, HashMap<(String, String), usize>) {
    let index = columns.iter().enumerate().map(|(i, col)| (col.qualified_name(), i)).collect();
    let frame_index = columns
        .iter()
        .enumerate()
        .filter_map(|(i, col)| col.table().map(|sf| ((sf.to_string(), col.name().to_string()), i)))
        .collect();
    (index, frame_index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Date, DateTime, Interval, Time};

    #[test]
    fn test_single_row_temporal_types() {
        let date = Date::from_ymd(2025, 1, 15).unwrap();
        let datetime = DateTime::from_timestamp(1642694400).unwrap();
        let time = Time::from_hms(14, 30, 45).unwrap();
        let interval = Interval::from_days(30);

        let frame = Frame::single_row([
            ("date_col", Value::Date(date.clone())),
            ("datetime_col", Value::DateTime(datetime.clone())),
            ("time_col", Value::Time(time.clone())),
            ("interval_col", Value::Interval(interval.clone())),
        ]);

        assert_eq!(frame.columns.len(), 4);
        assert_eq!(frame.shape(), (1, 4));

        // Check that the values are correctly stored
        assert_eq!(frame.column("date_col").unwrap().values().get(0), Value::Date(date));
        assert_eq!(
            frame.column("datetime_col").unwrap().values().get(0),
            Value::DateTime(datetime)
        );
        assert_eq!(frame.column("time_col").unwrap().values().get(0), Value::Time(time));
        assert_eq!(
            frame.column("interval_col").unwrap().values().get(0),
            Value::Interval(interval)
        );
    }

    #[test]
    fn test_single_row_mixed_types() {
        let date = Date::from_ymd(2025, 7, 15).unwrap();
        let time = Time::from_hms(9, 15, 30).unwrap();

        let frame = Frame::single_row([
            ("bool_col", Value::Bool(true)),
            ("int_col", Value::Int4(42)),
            ("str_col", Value::Utf8("hello".to_string())),
            ("date_col", Value::Date(date.clone())),
            ("time_col", Value::Time(time.clone())),
            ("undefined_col", Value::Undefined),
        ]);

        assert_eq!(frame.columns.len(), 6);
        assert_eq!(frame.shape(), (1, 6));

        // Check all values are correctly stored
        assert_eq!(frame.column("bool_col").unwrap().values().get(0), Value::Bool(true));
        assert_eq!(frame.column("int_col").unwrap().values().get(0), Value::Int4(42));
        assert_eq!(
            frame.column("str_col").unwrap().values().get(0),
            Value::Utf8("hello".to_string())
        );
        assert_eq!(frame.column("date_col").unwrap().values().get(0), Value::Date(date));
        assert_eq!(frame.column("time_col").unwrap().values().get(0), Value::Time(time));
        assert_eq!(frame.column("undefined_col").unwrap().values().get(0), Value::Undefined);
    }

    #[test]
    fn test_single_row_normal_column_names_work() {
        let frame = Frame::single_row([("normal_column", Value::Int4(42))]);
        assert_eq!(frame.columns.len(), 1);
        assert_eq!(frame.column("normal_column").unwrap().values().get(0), Value::Int4(42));
    }
}
