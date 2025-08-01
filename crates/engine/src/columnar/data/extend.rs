// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::ColumnData;
use reifydb_core::result::error::diagnostic::engine;
use reifydb_core::return_error;
use reifydb_core::value::container::{
    BlobContainer, BoolContainer, NumberContainer, StringContainer, TemporalContainer,
    UuidContainer,
};

impl ColumnData {
    pub fn extend(&mut self, other: ColumnData) -> crate::Result<()> {
        match (&mut *self, other) {
            // Same type extensions - delegate to container extend method
            (ColumnData::Bool(l), ColumnData::Bool(r)) => l.extend(&r)?,
            (ColumnData::Float4(l), ColumnData::Float4(r)) => l.extend(&r)?,
            (ColumnData::Float8(l), ColumnData::Float8(r)) => l.extend(&r)?,
            (ColumnData::Int1(l), ColumnData::Int1(r)) => l.extend(&r)?,
            (ColumnData::Int2(l), ColumnData::Int2(r)) => l.extend(&r)?,
            (ColumnData::Int4(l), ColumnData::Int4(r)) => l.extend(&r)?,
            (ColumnData::Int8(l), ColumnData::Int8(r)) => l.extend(&r)?,
            (ColumnData::Int16(l), ColumnData::Int16(r)) => l.extend(&r)?,
            (ColumnData::Uint1(l), ColumnData::Uint1(r)) => l.extend(&r)?,
            (ColumnData::Uint2(l), ColumnData::Uint2(r)) => l.extend(&r)?,
            (ColumnData::Uint4(l), ColumnData::Uint4(r)) => l.extend(&r)?,
            (ColumnData::Uint8(l), ColumnData::Uint8(r)) => l.extend(&r)?,
            (ColumnData::Uint16(l), ColumnData::Uint16(r)) => l.extend(&r)?,
            (ColumnData::Utf8(l), ColumnData::Utf8(r)) => l.extend(&r)?,
            (ColumnData::Date(l), ColumnData::Date(r)) => l.extend(&r)?,
            (ColumnData::DateTime(l), ColumnData::DateTime(r)) => l.extend(&r)?,
            (ColumnData::Time(l), ColumnData::Time(r)) => l.extend(&r)?,
            (ColumnData::Interval(l), ColumnData::Interval(r)) => l.extend(&r)?,
            (ColumnData::RowId(l), ColumnData::RowId(r)) => l.extend(&r)?,
            (ColumnData::Uuid4(l), ColumnData::Uuid4(r)) => l.extend(&r)?,
            (ColumnData::Uuid7(l), ColumnData::Uuid7(r)) => l.extend(&r)?,
            (ColumnData::Blob(l), ColumnData::Blob(r)) => l.extend(&r)?,
            (ColumnData::Undefined(l), ColumnData::Undefined(r)) => l.extend(&r)?,

            // Promote Undefined to typed
            (ColumnData::Undefined(l_container), typed_r) => {
                let l_len = l_container.len();
                match typed_r {
                    ColumnData::Bool(r) => {
                        let mut new_container = BoolContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Bool(new_container);
                    }
                    ColumnData::Float4(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Float4(new_container);
                    }
                    ColumnData::Float8(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Float8(new_container);
                    }
                    ColumnData::Int1(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Int1(new_container);
                    }
                    ColumnData::Int2(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Int2(new_container);
                    }
                    ColumnData::Int4(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Int4(new_container);
                    }
                    ColumnData::Int8(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Int8(new_container);
                    }
                    ColumnData::Int16(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Int16(new_container);
                    }
                    ColumnData::Uint1(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Uint1(new_container);
                    }
                    ColumnData::Uint2(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Uint2(new_container);
                    }
                    ColumnData::Uint4(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Uint4(new_container);
                    }
                    ColumnData::Uint8(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Uint8(new_container);
                    }
                    ColumnData::Uint16(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Uint16(new_container);
                    }
                    ColumnData::Utf8(r) => {
                        let mut new_container = StringContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Utf8(new_container);
                    }
                    ColumnData::Date(r) => {
                        let mut new_container = TemporalContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Date(new_container);
                    }
                    ColumnData::DateTime(r) => {
                        let mut new_container = TemporalContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::DateTime(new_container);
                    }
                    ColumnData::Time(r) => {
                        let mut new_container = TemporalContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Time(new_container);
                    }
                    ColumnData::Interval(r) => {
                        let mut new_container = TemporalContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Interval(new_container);
                    }
                    ColumnData::Uuid4(r) => {
                        let mut new_container = UuidContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Uuid4(new_container);
                    }
                    ColumnData::Uuid7(r) => {
                        let mut new_container = UuidContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Uuid7(new_container);
                    }
                    ColumnData::Blob(r) => {
                        let mut new_container = BlobContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnData::Blob(new_container);
                    }
                    ColumnData::RowId(_) => {
                        return_error!(engine::frame_error(
                            "Cannot extend RowId column from Undefined".to_string()
                        ));
                    }
                    ColumnData::Undefined(_) => {}
                }
            }

            // Extend typed with Undefined
            (typed_l, ColumnData::Undefined(r_container)) => {
                let r_len = r_container.len();
                match typed_l {
                    ColumnData::Bool(l) => l.extend_from_undefined(r_len),
                    ColumnData::Float4(l) => l.extend_from_undefined(r_len),
                    ColumnData::Float8(l) => l.extend_from_undefined(r_len),
                    ColumnData::Int1(l) => l.extend_from_undefined(r_len),
                    ColumnData::Int2(l) => l.extend_from_undefined(r_len),
                    ColumnData::Int4(l) => l.extend_from_undefined(r_len),
                    ColumnData::Int8(l) => l.extend_from_undefined(r_len),
                    ColumnData::Int16(l) => l.extend_from_undefined(r_len),
                    ColumnData::Uint1(l) => l.extend_from_undefined(r_len),
                    ColumnData::Uint2(l) => l.extend_from_undefined(r_len),
                    ColumnData::Uint4(l) => l.extend_from_undefined(r_len),
                    ColumnData::Uint8(l) => l.extend_from_undefined(r_len),
                    ColumnData::Uint16(l) => l.extend_from_undefined(r_len),
                    ColumnData::Utf8(l) => l.extend_from_undefined(r_len),
                    ColumnData::Date(l) => l.extend_from_undefined(r_len),
                    ColumnData::DateTime(l) => l.extend_from_undefined(r_len),
                    ColumnData::Time(l) => l.extend_from_undefined(r_len),
                    ColumnData::Interval(l) => l.extend_from_undefined(r_len),
                    ColumnData::RowId(l) => l.extend_from_undefined(r_len),
                    ColumnData::Uuid4(l) => l.extend_from_undefined(r_len),
                    ColumnData::Uuid7(l) => l.extend_from_undefined(r_len),
                    ColumnData::Blob(l) => l.extend_from_undefined(r_len),
                    ColumnData::Undefined(_) => unreachable!(),
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
