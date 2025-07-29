// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Push};
use crate::value::number::{SafeConvert, SafeDemote, SafePromote};

impl Push<i32> for ColumnValues {
    fn push(&mut self, value: i32) {
        match self {
            ColumnValues::Float4(container) => match value.checked_convert() {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnValues::Float8(container) => match value.checked_convert() {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnValues::Int1(container) => match value.checked_demote() {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnValues::Int2(container) => match value.checked_demote() {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnValues::Int4(container) => container.push(value),
            ColumnValues::Int8(container) => match value.checked_promote() {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnValues::Int16(container) => match value.checked_promote() {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnValues::Uint4(container) => match value.checked_convert() {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnValues::Uint8(container) => match value.checked_convert() {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnValues::Uint16(container) => match value.checked_convert() {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnValues::Undefined(container) => {
                let mut new_container = ColumnValues::int4(vec![0i32; container.len()]);
                if let ColumnValues::Int4(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => {
                panic!(
                    "called `push::<i32>()` on incompatible ColumnValues::{:?}",
                    other.get_type()
                );
            }
        }
    }
}
