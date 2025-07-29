// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::BitVec;
use crate::frame::ColumnValues;
use crate::frame::column::push::Push;
use crate::value::{Uuid4, Uuid7};

impl Push<Uuid4> for ColumnValues {
    fn push(&mut self, value: Uuid4) {
        match self {
            ColumnValues::Uuid4(values, bitvec) => {
                values.push(value);
                bitvec.push(true);
            }
            ColumnValues::Undefined(len) => {
                let mut values = vec![Uuid4::default(); *len];
                let mut bitvec = BitVec::repeat(*len, false);
                values.push(value);
                bitvec.push(true);
                *self = ColumnValues::uuid4_with_bitvec(values, bitvec);
            }
            other => {
                panic!(
                    "called `push::<Uuid4>()` on incompatible ColumnValues::{:?}",
                    other.get_type()
                );
            }
        }
    }
}

impl Push<Uuid7> for ColumnValues {
    fn push(&mut self, value: Uuid7) {
        match self {
            ColumnValues::Uuid7(values, bitvec) => {
                values.push(value);
                bitvec.push(true);
            }
            ColumnValues::Undefined(len) => {
                let mut values = vec![Uuid7::default(); *len];
                let mut bitvec = BitVec::repeat(*len, false);
                values.push(value);
                bitvec.push(true);
                *self = ColumnValues::uuid7_with_bitvec(values, bitvec);
            }
            other => {
                panic!(
                    "called `push::<Uuid4>()` on incompatible ColumnValues::{:?}",
                    other.get_type()
                );
            }
        }
    }
}
