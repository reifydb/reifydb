// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Push};
use crate::value::number::{SafeConvert, SafePromote};
use crate::{BitVec, CowVec};

impl Push<u8> for ColumnValues {
    fn push(&mut self, value: u8) {
        match self {
            ColumnValues::Float4(values, bitvec) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0.0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Float8(values, bitvec) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0.0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Uint1(values, bitvec) => {
                values.push(value);
                bitvec.push(true);
            }
            ColumnValues::Uint2(values, bitvec) => match value.checked_promote() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Uint4(values, bitvec) => match value.checked_promote() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Uint8(values, bitvec) => match value.checked_promote() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Uint16(values, bitvec) => match value.checked_promote() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Int1(values, bitvec) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Int2(values, bitvec) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Int4(values, bitvec) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Int8(values, bitvec) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Int16(values, bitvec) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Undefined(len) => {
                let mut values = vec![0u8; *len];
                let mut bitvec = BitVec::repeat(*len, false);
                values.push(value);
                bitvec.push(true);
                *self = ColumnValues::Uint1(CowVec::new(values), bitvec);
            }
            other => {
                panic!("called `push::<u8>()` on incompatible ColumnValues::{:?}", other.get_type());
            }
        }
    }
}
