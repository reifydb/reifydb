// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{ColumnValues, Push};
use reifydb_core::CowVec;
use reifydb_core::num::{SafeConvert, SafeDemote, SafePromote};

impl Push<i32> for ColumnValues {
    fn push(&mut self, value: i32) {
        match self {
            ColumnValues::Float4(values, validity) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0.0);
                    validity.push(false);
                }
            },
            ColumnValues::Float8(values, validity) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0.0);
                    validity.push(false);
                }
            },
            ColumnValues::Int1(values, validity) => match value.checked_demote() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Int2(values, validity) => match value.checked_demote() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Int4(values, validity) => {
                values.push(value);
                validity.push(true);
            }
            ColumnValues::Int8(values, validity) => match value.checked_promote() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Int16(values, validity) => match value.checked_promote() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Uint4(values, validity) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Uint8(values, validity) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Uint16(values, validity) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Undefined(len) => {
                let mut values = vec![0i32; *len];
                let mut validity = vec![false; *len];
                values.push(value);
                validity.push(true);
                *self = ColumnValues::Int4(CowVec::new(values), CowVec::new(validity));
            }
            other => {
                panic!("called `push::<i32>()` on incompatible ColumnValues::{:?}", other.kind());
            }
        }
    }
}
