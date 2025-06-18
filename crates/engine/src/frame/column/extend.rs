// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Column, ColumnValues};
use reifydb_core::CowVec;

impl Column {
    pub fn extend(&mut self, other: Column) -> crate::frame::Result<()> {
        self.data.extend(other.data)
    }
}

impl ColumnValues {
    pub fn extend(&mut self, other: ColumnValues) -> crate::frame::Result<()> {
        match (&mut *self, other) {
            (ColumnValues::Bool(l, l_valid), ColumnValues::Bool(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::Float4(l, l_valid), ColumnValues::Float4(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::Float8(l, l_valid), ColumnValues::Float8(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::Int1(l, l_valid), ColumnValues::Int1(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::Int2(l, l_valid), ColumnValues::Int2(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::Int4(l, l_valid), ColumnValues::Int4(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::Int8(l, l_valid), ColumnValues::Int8(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::Int16(l, l_valid), ColumnValues::Int16(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::String(l, l_valid), ColumnValues::String(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::Uint1(l, l_valid), ColumnValues::Uint1(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::Uint2(l, l_valid), ColumnValues::Uint2(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::Uint4(l, l_valid), ColumnValues::Uint4(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::Uint8(l, l_valid), ColumnValues::Uint8(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::Uint16(l, l_valid), ColumnValues::Uint16(r, r_valid)) => {
                l.extend(r);
                l_valid.extend(r_valid);
            }

            (ColumnValues::Undefined(l_len), ColumnValues::Undefined(r_len)) => {
                *l_len += r_len;
            }

            // Promote Undefined
            (ColumnValues::Undefined(l_len), typed_lr) => match typed_lr {
                ColumnValues::Bool(r, r_valid) => {
                    let mut values = CowVec::new(vec![false; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::bool_with_validity(values, validity);
                }
                ColumnValues::Float4(r, r_valid) => {
                    let mut values = CowVec::new(vec![0.0f32; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::float4_with_validity(values, validity);
                }
                ColumnValues::Float8(r, r_valid) => {
                    let mut values = CowVec::new(vec![0.0f64; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::float8_with_validity(values, validity);
                }
                ColumnValues::Int1(r, r_valid) => {
                    let mut values = CowVec::new(vec![0i8; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::int1_with_validity(values, validity);
                }
                ColumnValues::Int2(r, r_valid) => {
                    let mut values = CowVec::new(vec![0i16; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::int2_with_validity(values, validity);
                }
                ColumnValues::Int4(r, r_valid) => {
                    let mut values = CowVec::new(vec![0i32; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::int4_with_validity(values, validity);
                }
                ColumnValues::Int8(r, r_valid) => {
                    let mut values = CowVec::new(vec![0i64; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::int8_with_validity(values, validity);
                }
                ColumnValues::Int16(r, r_valid) => {
                    let mut values = CowVec::new(vec![0i128; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::int16_with_validity(values, validity);
                }
                ColumnValues::String(r, r_valid) => {
                    let mut values = CowVec::new(vec!["".to_string(); *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::string_with_validity(values, validity);
                }
                ColumnValues::Uint1(r, r_valid) => {
                    let mut values = CowVec::new(vec![0u8; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::uint1_with_validity(values, validity);
                }
                ColumnValues::Uint2(r, r_valid) => {
                    let mut values = CowVec::new(vec![0u16; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::uint2_with_validity(values, validity);
                }
                ColumnValues::Uint4(r, r_valid) => {
                    let mut values = CowVec::new(vec![0u32; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::uint4_with_validity(values, validity);
                }
                ColumnValues::Uint8(r, r_valid) => {
                    let mut values = CowVec::new(vec![0u64; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::uint8_with_validity(values, validity);
                }
                ColumnValues::Uint16(r, r_valid) => {
                    let mut values = CowVec::new(vec![0u128; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(r_valid);

                    *self = ColumnValues::uint16_with_validity(values, validity);
                }
                ColumnValues::Undefined(_) => {}
            },

            // Prevent appending typed into Undefined
            (typed_l, ColumnValues::Undefined(r_len)) => match typed_l {
                ColumnValues::Bool(l, l_valid) => {
                    l.extend(std::iter::repeat(false).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Float4(l, l_valid) => {
                    l.extend(std::iter::repeat(0.0f32).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Float8(l, l_valid) => {
                    l.extend(std::iter::repeat(0.0f64).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Int1(l, l_valid) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Int2(l, l_valid) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Int4(l, l_valid) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Int8(l, l_valid) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Int16(l, l_valid) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::String(l, l_valid) => {
                    l.extend(std::iter::repeat(String::new()).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Uint1(l, l_valid) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Uint2(l, l_valid) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Uint4(l, l_valid) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Uint8(l, l_valid) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Uint16(l, l_valid) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    l_valid.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Undefined(_) => unreachable!(),
            },

            (_, _) => {
                return Err("column type mismatch".to_string().into());
            }
        }

        Ok(())
    }
}
