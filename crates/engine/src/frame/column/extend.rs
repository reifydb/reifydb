// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Column, ColumnValues};
use reifydb_core::CowVec;

impl Column {
    pub fn extend(&mut self, other: Column) -> crate::frame::Result<()> {
        self.values.extend(other.values)
    }
}

impl ColumnValues {
    pub fn extend(&mut self, other: ColumnValues) -> crate::frame::Result<()> {
        match (&mut *self, other) {
            (ColumnValues::Bool(l, lv), ColumnValues::Bool(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::Float4(l, lv), ColumnValues::Float4(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::Float8(l, lv), ColumnValues::Float8(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::Int1(l, lv), ColumnValues::Int1(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::Int2(l, lv), ColumnValues::Int2(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::Int4(l, lv), ColumnValues::Int4(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::Int8(l, lv), ColumnValues::Int8(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::Int16(l, lv), ColumnValues::Int16(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::String(l, lv), ColumnValues::String(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::Uint1(l, lv), ColumnValues::Uint1(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::Uint2(l, lv), ColumnValues::Uint2(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::Uint4(l, lv), ColumnValues::Uint4(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::Uint8(l, lv), ColumnValues::Uint8(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::Uint16(l, lv), ColumnValues::Uint16(r, rv)) => {
                l.extend(r);
                lv.extend(rv);
            }

            (ColumnValues::Undefined(l_len), ColumnValues::Undefined(r_len)) => {
                *l_len += r_len;
            }

            // Promote Undefined
            (ColumnValues::Undefined(l_len), typed_lr) => match typed_lr {
                ColumnValues::Bool(r, rv) => {
                    let mut values = CowVec::new(vec![false; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::bool_with_validity(values, validity);
                }
                ColumnValues::Float4(r, rv) => {
                    let mut values = CowVec::new(vec![0.0f32; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::float4_with_validity(values, validity);
                }
                ColumnValues::Float8(r, rv) => {
                    let mut values = CowVec::new(vec![0.0f64; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::float8_with_validity(values, validity);
                }
                ColumnValues::Int1(r, rv) => {
                    let mut values = CowVec::new(vec![0i8; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::int1_with_validity(values, validity);
                }
                ColumnValues::Int2(r, rv) => {
                    let mut values = CowVec::new(vec![0i16; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::int2_with_validity(values, validity);
                }
                ColumnValues::Int4(r, rv) => {
                    let mut values = CowVec::new(vec![0i32; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::int4_with_validity(values, validity);
                }
                ColumnValues::Int8(r, rv) => {
                    let mut values = CowVec::new(vec![0i64; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::int8_with_validity(values, validity);
                }
                ColumnValues::Int16(r, rv) => {
                    let mut values = CowVec::new(vec![0i128; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::int16_with_validity(values, validity);
                }
                ColumnValues::String(r, rv) => {
                    let mut values = CowVec::new(vec!["".to_string(); *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::string_with_validity(values, validity);
                }
                ColumnValues::Uint1(r, rv) => {
                    let mut values = CowVec::new(vec![0u8; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::uint1_with_validity(values, validity);
                }
                ColumnValues::Uint2(r, rv) => {
                    let mut values = CowVec::new(vec![0u16; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::uint2_with_validity(values, validity);
                }
                ColumnValues::Uint4(r, rv) => {
                    let mut values = CowVec::new(vec![0u32; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::uint4_with_validity(values, validity);
                }
                ColumnValues::Uint8(r, rv) => {
                    let mut values = CowVec::new(vec![0u64; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::uint8_with_validity(values, validity);
                }
                ColumnValues::Uint16(r, rv) => {
                    let mut values = CowVec::new(vec![0u128; *l_len]);
                    values.extend(r);

                    let mut validity = CowVec::new(vec![false; *l_len]);
                    validity.extend(rv);

                    *self = ColumnValues::uint16_with_validity(values, validity);
                }
                ColumnValues::Undefined(_) => {}
            },

            // Prevent appending typed into Undefined
            (typed_l, ColumnValues::Undefined(r_len)) => match typed_l {
                ColumnValues::Bool(l, lv) => {
                    l.extend(std::iter::repeat(false).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Float4(l, lv) => {
                    l.extend(std::iter::repeat(0.0f32).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Float8(l, lv) => {
                    l.extend(std::iter::repeat(0.0f64).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Int1(l, lv) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Int2(l, lv) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Int4(l, lv) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Int8(l, lv) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Int16(l, lv) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::String(l, lv) => {
                    l.extend(std::iter::repeat(String::new()).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Uint1(l, lv) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Uint2(l, lv) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Uint4(l, lv) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Uint8(l, lv) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
                }
                ColumnValues::Uint16(l, lv) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lv.extend(std::iter::repeat(false).take(r_len));
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
