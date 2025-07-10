// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{ColumnValues, Push};
use reifydb_core::CowVec;
use reifydb_core::num::{SafeConvert, SafePromote};

impl Push<u8> for ColumnValues {
    fn push(&mut self, value: u8) {
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
            ColumnValues::Uint1(values, validity) => {
                values.push(value);
                validity.push(true);
            }
            ColumnValues::Uint2(values, validity) => match value.checked_promote() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Uint4(values, validity) => match value.checked_promote() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Uint8(values, validity) => match value.checked_promote() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Uint16(values, validity) => match value.checked_promote() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Int1(values, validity) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Int2(values, validity) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Int4(values, validity) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Int8(values, validity) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Int16(values, validity) => match value.checked_convert() {
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
                let mut values = vec![0u8; *len];
                let mut validity = vec![false; *len];
                values.push(value);
                validity.push(true);
                *self = ColumnValues::Uint1(CowVec::new(values), CowVec::new(validity));
            }
            other => {
                panic!("called `push::<u8>()` on incompatible ColumnValues::{:?}", other.data_type());
            }
        }
    }
}
