// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::{ColumnQualified, EngineColumn, EngineColumnData, TableQualified};
use reifydb_core::error::diagnostic::engine;
use reifydb_core::interface::Table;
use reifydb_core::{Type, Value, return_error};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    pub name: String,
    pub columns: Vec<EngineColumn>,
    pub index: HashMap<String, usize>, // Maps qualified_name -> column index
    pub frame_index: HashMap<(String, String), usize>, // Maps (frame, name) -> index
}

impl Frame {
    pub fn single_row<'a>(rows: impl IntoIterator<Item = (&'a str, Value)>) -> Frame {
        let mut columns = Vec::new();
        let mut index = HashMap::new();

        for (idx, (name, value)) in rows.into_iter().enumerate() {
            let data = match value {
                Value::Undefined => EngineColumnData::undefined(1),
                Value::Bool(v) => EngineColumnData::bool([v]),
                Value::Float4(v) => EngineColumnData::float4([v.into()]),
                Value::Float8(v) => EngineColumnData::float8([v.into()]),
                Value::Int1(v) => EngineColumnData::int1([v]),
                Value::Int2(v) => EngineColumnData::int2([v]),
                Value::Int4(v) => EngineColumnData::int4([v]),
                Value::Int8(v) => EngineColumnData::int8([v]),
                Value::Int16(v) => EngineColumnData::int16([v]),
                Value::Utf8(ref v) => EngineColumnData::utf8([v.clone()]),
                Value::Uint1(v) => EngineColumnData::uint1([v]),
                Value::Uint2(v) => EngineColumnData::uint2([v]),
                Value::Uint4(v) => EngineColumnData::uint4([v]),
                Value::Uint8(v) => EngineColumnData::uint8([v]),
                Value::Uint16(v) => EngineColumnData::uint16([v]),
                Value::Date(ref v) => EngineColumnData::date([v.clone()]),
                Value::DateTime(ref v) => EngineColumnData::datetime([v.clone()]),
                Value::Time(ref v) => EngineColumnData::time([v.clone()]),
                Value::Interval(ref v) => EngineColumnData::interval([v.clone()]),
                Value::RowId(v) => EngineColumnData::row_id([v]),
                Value::Uuid4(v) => EngineColumnData::uuid4([v]),
                Value::Uuid7(v) => EngineColumnData::uuid7([v]),
                Value::Blob(ref v) => EngineColumnData::blob([v.clone()]),
            };

            let column =
                EngineColumn::ColumnQualified(ColumnQualified { name: name.to_string(), data });
            index.insert(column.qualified_name(), idx);
            columns.push(column);
        }

        let (final_index, frame_index) = build_indices(&columns);
        Frame { name: "frame".to_string(), columns, index: final_index, frame_index }
    }
}

impl Frame {
    pub fn new(columns: Vec<EngineColumn>) -> Self {
        let n = columns.first().map_or(0, |c| c.data().len());
        assert!(columns.iter().all(|c| c.data().len() == n));

        let (index, frame_index) = build_indices(&columns);
        Self { name: "frame".to_string(), columns, index, frame_index }
    }

    pub fn new_with_name(columns: Vec<EngineColumn>, name: impl Into<String>) -> Self {
        let n = columns.first().map_or(0, |c| c.data().len());
        assert!(columns.iter().all(|c| c.data().len() == n));

        let (index, frame_index) = build_indices(&columns);
        Self { name: name.into(), columns, index, frame_index }
    }

    pub fn shape(&self) -> (usize, usize) {
        (self.columns.get(0).map(|c| c.data().len()).unwrap_or(0), self.columns.len())
    }

    pub fn is_empty(&self) -> bool {
        self.shape().0 == 0
    }

    pub fn row(&self, i: usize) -> Vec<Value> {
        self.columns.iter().map(|c| c.data().get_value(i)).collect()
    }

    pub fn column(&self, name: &str) -> Option<&EngineColumn> {
        // Try qualified name first, then try as original name
        self.index
            .get(name)
            .map(|&i| &self.columns[i])
            .or_else(|| self.columns.iter().find(|col| col.name() == name))
    }

    pub fn column_by_source(&self, frame: &str, name: &str) -> Option<&EngineColumn> {
        self.frame_index.get(&(frame.to_string(), name.to_string())).map(|&i| &self.columns[i])
    }

    pub fn column_values(&self, name: &str) -> Option<&EngineColumnData> {
        // Try qualified name first, then try as original name
        self.index
            .get(name)
            .map(|&i| self.columns[i].data())
            .or_else(|| self.columns.iter().find(|col| col.name() == name).map(|col| col.data()))
    }

    pub fn column_values_mut(&mut self, name: &str) -> Option<&mut EngineColumnData> {
        // Try qualified name first, then try as original name
        if let Some(&i) = self.index.get(name) {
            Some(self.columns[i].data_mut())
        } else {
            let pos = self.columns.iter().position(|col| col.name() == name)?;
            Some(self.columns[pos].data_mut())
        }
    }

    pub fn row_count(&self) -> usize {
        self.columns.first().map_or(0, |col| col.data().len())
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_row(&self, index: usize) -> Vec<Value> {
        self.columns.iter().map(|col| col.data().get_value(index)).collect()
    }
}

impl EngineColumn {
    pub fn extend(&mut self, other: EngineColumn) -> crate::Result<()> {
        self.data_mut().extend(other.data().clone())
    }
}

impl Frame {
    pub fn from_rows(names: &[&str], result_rows: &[Vec<Value>]) -> Self {
        let column_count = names.len();

        let mut columns: Vec<EngineColumn> = names
            .iter()
            .map(|name| {
                EngineColumn::ColumnQualified(ColumnQualified {
                    name: name.to_string(),
                    data: EngineColumnData::with_capacity(Type::Undefined, 0),
                })
            })
            .collect();

        for row in result_rows {
            assert_eq!(row.len(), column_count, "row length does not match column count");
            for (i, value) in row.iter().enumerate() {
                columns[i].data_mut().push_value(value.clone());
            }
        }

        Frame::new(columns)
    }
}


impl Frame {
    pub fn empty() -> Self {
        Self {
            name: "frame".to_string(),
            columns: vec![],
            index: HashMap::new(),
            frame_index: HashMap::new(),
        }
    }

    pub fn empty_from_table(table: &Table) -> Self {
        let columns: Vec<EngineColumn> = table
            .columns
            .iter()
            .map(|col| {
                let name = col.name.clone();
                let data = match col.ty {
                    Type::Bool => EngineColumnData::bool(vec![]),
                    Type::Float4 => EngineColumnData::float4(vec![]),
                    Type::Float8 => EngineColumnData::float8(vec![]),
                    Type::Int1 => EngineColumnData::int1(vec![]),
                    Type::Int2 => EngineColumnData::int2(vec![]),
                    Type::Int4 => EngineColumnData::int4(vec![]),
                    Type::Int8 => EngineColumnData::int8(vec![]),
                    Type::Int16 => EngineColumnData::int16(vec![]),
                    Type::Utf8 => EngineColumnData::utf8(Vec::<String>::new()),
                    Type::Uint1 => EngineColumnData::uint1(vec![]),
                    Type::Uint2 => EngineColumnData::uint2(vec![]),
                    Type::Uint4 => EngineColumnData::uint4(vec![]),
                    Type::Uint8 => EngineColumnData::uint8(vec![]),
                    Type::Uint16 => EngineColumnData::uint16(vec![]),
                    Type::Date => EngineColumnData::date(vec![]),
                    Type::DateTime => EngineColumnData::datetime(vec![]),
                    Type::Time => EngineColumnData::time(vec![]),
                    Type::Interval => EngineColumnData::interval(vec![]),
                    Type::RowId => EngineColumnData::row_id(vec![]),
                    Type::Uuid4 => EngineColumnData::uuid4(vec![]),
                    Type::Uuid7 => EngineColumnData::uuid7(vec![]),
                    Type::Blob => EngineColumnData::blob(vec![]),
                    Type::Undefined => EngineColumnData::undefined(0),
                };
                EngineColumn::TableQualified(TableQualified {
                    table: table.name.clone(),
                    name,
                    data,
                })
            })
            .collect();

        Self::new_with_name(columns, table.name.clone())
    }
}

pub(crate) fn build_indices(
    columns: &[EngineColumn],
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
    use reifydb_core::{Date, DateTime, Interval, Time};

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
        assert_eq!(frame.column("date_col").unwrap().data().get_value(0), Value::Date(date));
        assert_eq!(
            frame.column("datetime_col").unwrap().data().get_value(0),
            Value::DateTime(datetime)
        );
        assert_eq!(frame.column("time_col").unwrap().data().get_value(0), Value::Time(time));
        assert_eq!(
            frame.column("interval_col").unwrap().data().get_value(0),
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
        assert_eq!(frame.column("bool_col").unwrap().data().get_value(0), Value::Bool(true));
        assert_eq!(frame.column("int_col").unwrap().data().get_value(0), Value::Int4(42));
        assert_eq!(
            frame.column("str_col").unwrap().data().get_value(0),
            Value::Utf8("hello".to_string())
        );
        assert_eq!(frame.column("date_col").unwrap().data().get_value(0), Value::Date(date));
        assert_eq!(frame.column("time_col").unwrap().data().get_value(0), Value::Time(time));
        assert_eq!(frame.column("undefined_col").unwrap().data().get_value(0), Value::Undefined);
    }

    #[test]
    fn test_single_row_normal_column_names_work() {
        let frame = Frame::single_row([("normal_column", Value::Int4(42))]);
        assert_eq!(frame.columns.len(), 1);
        assert_eq!(frame.column("normal_column").unwrap().data().get_value(0), Value::Int4(42));
    }
}
