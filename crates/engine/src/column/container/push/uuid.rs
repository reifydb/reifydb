// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::EngineColumnData;
use crate::column::container::push::Push;
use reifydb_core::value::{Uuid4, Uuid7};

impl Push<Uuid4> for EngineColumnData {
    fn push(&mut self, value: Uuid4) {
        match self {
            EngineColumnData::Uuid4(container) => container.push(value),
            EngineColumnData::Undefined(container) => {
                let mut new_container =
                    EngineColumnData::uuid4(vec![Uuid4::default(); container.len()]);
                if let EngineColumnData::Uuid4(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => {
                panic!(
                    "called `push::<Uuid4>()` on incompatible EngineColumnData::{:?}",
                    other.get_type()
                );
            }
        }
    }
}

impl Push<Uuid7> for EngineColumnData {
    fn push(&mut self, value: Uuid7) {
        match self {
            EngineColumnData::Uuid7(container) => container.push(value),
            EngineColumnData::Undefined(container) => {
                let mut new_container =
                    EngineColumnData::uuid7(vec![Uuid7::default(); container.len()]);
                if let EngineColumnData::Uuid7(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => {
                panic!(
                    "called `push::<Uuid7>()` on incompatible EngineColumnData::{:?}",
                    other.get_type()
                );
            }
        }
    }
}
