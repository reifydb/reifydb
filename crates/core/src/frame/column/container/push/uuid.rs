// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::ColumnValues;
use crate::frame::column::container::push::Push;
use crate::value::{Uuid4, Uuid7};

impl Push<Uuid4> for ColumnValues {
    fn push(&mut self, value: Uuid4) {
        match self {
            ColumnValues::Uuid4(container) => container.push(value),
            ColumnValues::Undefined(container) => {
                let mut new_container = ColumnValues::uuid4(vec![Uuid4::default(); container.len()]);
                if let ColumnValues::Uuid4(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
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
            ColumnValues::Uuid7(container) => container.push(value),
            ColumnValues::Undefined(container) => {
                let mut new_container = ColumnValues::uuid7(vec![Uuid7::default(); container.len()]);
                if let ColumnValues::Uuid7(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => {
                panic!(
                    "called `push::<Uuid7>()` on incompatible ColumnValues::{:?}",
                    other.get_type()
                );
            }
        }
    }
}
