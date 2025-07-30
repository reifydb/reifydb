// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::ColumnValues;
use crate::column::container::{
    BlobContainer, BoolContainer, NumberContainer, StringContainer, TemporalContainer,
    UuidContainer,
};
use reifydb_core::error::diagnostic::engine;
use reifydb_core::return_error;

impl ColumnValues {
    pub fn extend(&mut self, other: ColumnValues) -> crate::Result<()> {
        match (&mut *self, other) {
            // Same type extensions - delegate to container extend method
            (ColumnValues::Bool(l), ColumnValues::Bool(r)) => l.extend(&r)?,
            (ColumnValues::Float4(l), ColumnValues::Float4(r)) => l.extend(&r)?,
            (ColumnValues::Float8(l), ColumnValues::Float8(r)) => l.extend(&r)?,
            (ColumnValues::Int1(l), ColumnValues::Int1(r)) => l.extend(&r)?,
            (ColumnValues::Int2(l), ColumnValues::Int2(r)) => l.extend(&r)?,
            (ColumnValues::Int4(l), ColumnValues::Int4(r)) => l.extend(&r)?,
            (ColumnValues::Int8(l), ColumnValues::Int8(r)) => l.extend(&r)?,
            (ColumnValues::Int16(l), ColumnValues::Int16(r)) => l.extend(&r)?,
            (ColumnValues::Uint1(l), ColumnValues::Uint1(r)) => l.extend(&r)?,
            (ColumnValues::Uint2(l), ColumnValues::Uint2(r)) => l.extend(&r)?,
            (ColumnValues::Uint4(l), ColumnValues::Uint4(r)) => l.extend(&r)?,
            (ColumnValues::Uint8(l), ColumnValues::Uint8(r)) => l.extend(&r)?,
            (ColumnValues::Uint16(l), ColumnValues::Uint16(r)) => l.extend(&r)?,
            (ColumnValues::Utf8(l), ColumnValues::Utf8(r)) => l.extend(&r)?,
            (ColumnValues::Date(l), ColumnValues::Date(r)) => l.extend(&r)?,
            (ColumnValues::DateTime(l), ColumnValues::DateTime(r)) => l.extend(&r)?,
            (ColumnValues::Time(l), ColumnValues::Time(r)) => l.extend(&r)?,
            (ColumnValues::Interval(l), ColumnValues::Interval(r)) => l.extend(&r)?,
            (ColumnValues::RowId(l), ColumnValues::RowId(r)) => l.extend(&r)?,
            (ColumnValues::Uuid4(l), ColumnValues::Uuid4(r)) => l.extend(&r)?,
            (ColumnValues::Uuid7(l), ColumnValues::Uuid7(r)) => l.extend(&r)?,
            (ColumnValues::Blob(l), ColumnValues::Blob(r)) => l.extend(&r)?,
            (ColumnValues::Undefined(l), ColumnValues::Undefined(r)) => l.extend(&r)?,

            // Promote Undefined to typed
            (ColumnValues::Undefined(l_container), typed_r) => {
                let l_len = l_container.len();
                match typed_r {
                    ColumnValues::Bool(r) => {
                        let mut new_container = BoolContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Bool(new_container);
                    }
                    ColumnValues::Float4(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Float4(new_container);
                    }
                    ColumnValues::Float8(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Float8(new_container);
                    }
                    ColumnValues::Int1(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Int1(new_container);
                    }
                    ColumnValues::Int2(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Int2(new_container);
                    }
                    ColumnValues::Int4(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Int4(new_container);
                    }
                    ColumnValues::Int8(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Int8(new_container);
                    }
                    ColumnValues::Int16(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Int16(new_container);
                    }
                    ColumnValues::Uint1(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Uint1(new_container);
                    }
                    ColumnValues::Uint2(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Uint2(new_container);
                    }
                    ColumnValues::Uint4(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Uint4(new_container);
                    }
                    ColumnValues::Uint8(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Uint8(new_container);
                    }
                    ColumnValues::Uint16(r) => {
                        let mut new_container = NumberContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Uint16(new_container);
                    }
                    ColumnValues::Utf8(r) => {
                        let mut new_container = StringContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Utf8(new_container);
                    }
                    ColumnValues::Date(r) => {
                        let mut new_container = TemporalContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Date(new_container);
                    }
                    ColumnValues::DateTime(r) => {
                        let mut new_container = TemporalContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::DateTime(new_container);
                    }
                    ColumnValues::Time(r) => {
                        let mut new_container = TemporalContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Time(new_container);
                    }
                    ColumnValues::Interval(r) => {
                        let mut new_container = TemporalContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Interval(new_container);
                    }
                    ColumnValues::Uuid4(r) => {
                        let mut new_container = UuidContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Uuid4(new_container);
                    }
                    ColumnValues::Uuid7(r) => {
                        let mut new_container = UuidContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Uuid7(new_container);
                    }
                    ColumnValues::Blob(r) => {
                        let mut new_container = BlobContainer::with_capacity(l_len + r.len());
                        new_container.extend_from_undefined(l_len);
                        new_container.extend(&r)?;
                        *self = ColumnValues::Blob(new_container);
                    }
                    ColumnValues::RowId(_) => {
                        return_error!(engine::frame_error(
                            "Cannot extend RowId column from Undefined".to_string()
                        ));
                    }
                    ColumnValues::Undefined(_) => {}
                }
            }

            // Extend typed with Undefined
            (typed_l, ColumnValues::Undefined(r_container)) => {
                let r_len = r_container.len();
                match typed_l {
                    ColumnValues::Bool(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Float4(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Float8(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Int1(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Int2(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Int4(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Int8(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Int16(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Uint1(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Uint2(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Uint4(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Uint8(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Uint16(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Utf8(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Date(l) => l.extend_from_undefined(r_len),
                    ColumnValues::DateTime(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Time(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Interval(l) => l.extend_from_undefined(r_len),
                    ColumnValues::RowId(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Uuid4(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Uuid7(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Blob(l) => l.extend_from_undefined(r_len),
                    ColumnValues::Undefined(_) => unreachable!(),
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
