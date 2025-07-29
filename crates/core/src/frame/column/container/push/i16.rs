// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Push};
use crate::value::number::{SafeConvert, SafeDemote, SafePromote};

impl Push<i16> for ColumnValues {
    fn push(&mut self, value: i16) {
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
            ColumnValues::Int2(container) => container.push(value),
            ColumnValues::Int4(container) => match value.checked_promote() {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnValues::Int8(container) => match value.checked_promote() {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnValues::Int16(container) => match value.checked_promote() {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnValues::Uint1(container) => match value.checked_convert() {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnValues::Uint2(container) => match value.checked_convert() {
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
                let mut new_container = ColumnValues::int2(vec![0i16; container.len()]);
                if let ColumnValues::Int2(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => {
                panic!("called `push::<i16>()` on incompatible ColumnValues::{:?}", other.get_type());
            }
        }
    }
}
