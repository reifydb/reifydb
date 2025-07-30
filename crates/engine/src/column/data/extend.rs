// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::EngineColumnData;
use crate::column::container::{
    BlobContainer, BoolContainer, NumberContainer, StringContainer, TemporalContainer,
    UuidContainer,
};
use reifydb_core::error::diagnostic::engine;
use reifydb_core::return_error;

impl EngineColumnData {
    pub fn extend(&mut self, other: EngineColumnData) -> crate::Result<()> {
        match (&mut *self, other) {
            // Same type extensions - delegate to container extend method
            (EngineColumnData::Bool(l), EngineColumnData::Bool(r)) => l.extend(&r)?,
            (EngineColumnData::Float4(l), EngineColumnData::Float4(r)) => l.extend(&r)?,
            (EngineColumnData::Float8(l), EngineColumnData::Float8(r)) => l.extend(&r)?,
            (EngineColumnData::Int1(l), EngineColumnData::Int1(r)) => l.extend(&r)?,
            (EngineColumnData::Int2(l), EngineColumnData::Int2(r)) => l.extend(&r)?,
            (EngineColumnData::Int4(l), EngineColumnData::Int4(r)) => l.extend(&r)?,
            (EngineColumnData::Int8(l), EngineColumnData::Int8(r)) => l.extend(&r)?,
            (EngineColumnData::Int16(l), EngineColumnData::Int16(r)) => l.extend(&r)?,
            (EngineColumnData::Uint1(l), EngineColumnData::Uint1(r)) => l.extend(&r)?,
            (EngineColumnData::Uint2(l), EngineColumnData::Uint2(r)) => l.extend(&r)?,
            (EngineColumnData::Uint4(l), EngineColumnData::Uint4(r)) => l.extend(&r)?,
            (EngineColumnData::Uint8(l), EngineColumnData::Uint8(r)) => l.extend(&r)?,
            (EngineColumnData::Uint16(l), EngineColumnData::Uint16(r)) => l.extend(&r)?,
            (EngineColumnData::Utf8(l), EngineColumnData::Utf8(r)) => l.extend(&r)?,
            (EngineColumnData::Date(l), EngineColumnData::Date(r)) => l.extend(&r)?,
            (EngineColumnData::DateTime(l), EngineColumnData::DateTime(r)) => l.extend(&r)?,
            (EngineColumnData::Time(l), EngineColumnData::Time(r)) => l.extend(&r)?,
            (EngineColumnData::Interval(l), EngineColumnData::Interval(r)) => l.extend(&r)?,
            (EngineColumnData::RowId(l), EngineColumnData::RowId(r)) => l.extend(&r)?,
            (EngineColumnData::Uuid4(l), EngineColumnData::Uuid4(r)) => l.extend(&r)?,
            (EngineColumnData::Uuid7(l), EngineColumnData::Uuid7(r)) => l.extend(&r)?,
            (EngineColumnData::Blob(l), EngineColumnData::Blob(r)) => l.extend(&r)?,
            (EngineColumnData::Undefined(l), EngineColumnData::Undefined(r)) => l.extend(&r)?,

            // Promote Undefined to typed
            (EngineColumnData::Undefined(l_container), typed_r) => {
                let l_len = l_container.len();
                match typed_r {
                    EngineColumnData::Bool(r) => {
                        let mut new_container = BoolContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Bool(new_container);
                    }
                    EngineColumnData::Float4(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Float4(new_container);
                    }
                    EngineColumnData::Float8(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Float8(new_container);
                    }
                    EngineColumnData::Int1(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Int1(new_container);
                    }
                    EngineColumnData::Int2(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Int2(new_container);
                    }
                    EngineColumnData::Int4(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Int4(new_container);
                    }
                    EngineColumnData::Int8(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Int8(new_container);
                    }
                    EngineColumnData::Int16(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Int16(new_container);
                    }
                    EngineColumnData::Uint1(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Uint1(new_container);
                    }
                    EngineColumnData::Uint2(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Uint2(new_container);
                    }
                    EngineColumnData::Uint4(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Uint4(new_container);
                    }
                    EngineColumnData::Uint8(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Uint8(new_container);
                    }
                    EngineColumnData::Uint16(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Uint16(new_container);
                    }
                    EngineColumnData::Utf8(r) => {
                        let mut new_container = StringContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Utf8(new_container);
                    }
                    EngineColumnData::Date(r) => {
                        let mut new_container = TemporalContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Date(new_container);
                    }
                    EngineColumnData::DateTime(r) => {
                        let mut new_container = TemporalContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::DateTime(new_container);
                    }
                    EngineColumnData::Time(r) => {
                        let mut new_container = TemporalContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Time(new_container);
                    }
                    EngineColumnData::Interval(r) => {
                        let mut new_container = TemporalContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Interval(new_container);
                    }
                    EngineColumnData::Uuid4(r) => {
                        let mut new_container = UuidContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Uuid4(new_container);
                    }
                    EngineColumnData::Uuid7(r) => {
                        let mut new_container = UuidContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Uuid7(new_container);
                    }
                    EngineColumnData::Blob(r) => {
                        let mut new_container = BlobContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = EngineColumnData::Blob(new_container);
                    }
                    EngineColumnData::RowId(_) => {
                        return_error!(engine::frame_error(
                            "Cannot extend RowId column from Undefined".to_string()
                        ));
                    }
                    EngineColumnData::Undefined(_) => {}
                }
            }

            // Extend typed with Undefined
            (typed_l, EngineColumnData::Undefined(r_container)) => {
                let r_len = r_container.len();
                match typed_l {
                    EngineColumnData::Bool(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Float4(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Float8(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Int1(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Int2(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Int4(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Int8(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Int16(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Uint1(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Uint2(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Uint4(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Uint8(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Uint16(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Utf8(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Date(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::DateTime(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Time(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Interval(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::RowId(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Uuid4(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Uuid7(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Blob(l) => l.extend_from_undefined(r_len),
                    EngineColumnData::Undefined(_) => unreachable!(),
                }
            }

            // Type mismatch
            (_, _) => {
                return_error!(engine::frame_error("column type mismatch".to_string()));
            }
        }

        Ok(())
    }
}
