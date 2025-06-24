// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{ColumnValues, Push};
use reifydb_core::CowVec;
use reifydb_core::num::{SafeConvert, SafePromote};

impl Push<i8> for ColumnValues {
    fn push(&mut self, value: i8) {
        match self {
            ColumnValues::Int1(values, validity) => {
                values.push(value);
                validity.push(true);
            }
            ColumnValues::Int2(values, validity) => {
                values.push(value.saturating_promote());
                validity.push(true);
            }
            ColumnValues::Int4(values, validity) => {
                values.push(value.saturating_promote());
                validity.push(true);
            }
            ColumnValues::Int8(values, validity) => {
                values.push(value.saturating_promote());
                validity.push(true);
            }
            ColumnValues::Int16(values, validity) => {
                values.push(value.saturating_promote());
                validity.push(true);
            }
            ColumnValues::Uint1(values, validity) => {
                values.push(value.saturating_convert());
                validity.push(true);
            }
            ColumnValues::Uint2(values, validity) => {
                values.push(value.saturating_convert());
                validity.push(true);
            }
            ColumnValues::Uint4(values, validity) => {
                values.push(value.saturating_convert());
                validity.push(true);
            }
            ColumnValues::Uint8(values, validity) => {
                values.push(value.saturating_convert());
                validity.push(true);
            }
            ColumnValues::Uint16(values, validity) => {
                values.push(value.saturating_convert());
                validity.push(true);
            }
            ColumnValues::Float4(values, validity) => {
                values.push(value.saturating_convert());
                validity.push(true);
            }
            ColumnValues::Float8(values, validity) => {
                values.push(value.saturating_convert());
                validity.push(true);
            }
            ColumnValues::Undefined(len) => {
                let mut values = vec![Default::default(); *len];
                let mut validity = vec![false; *len];
                values.push(value);
                validity.push(true);
                *self = ColumnValues::Int1(CowVec
                ::new(values), CowVec::new(validity));
            }
            other => {
                panic!("called `push::<i8>()` on incompatible ColumnValues::{:?}", other.kind());
            }
        }
    }
}
