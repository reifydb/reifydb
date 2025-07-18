// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, FrameColumn};
use reifydb_core::{BitVec, CowVec, Date, DateTime, Interval, Time};

impl FrameColumn {
    pub fn extend(&mut self, other: FrameColumn) -> crate::frame::Result<()> {
        self.values.extend(other.values)
    }
}

impl ColumnValues {
    pub fn extend(&mut self, other: ColumnValues) -> crate::frame::Result<()> {
        match (&mut *self, other) {
            (ColumnValues::Bool(l, lb), ColumnValues::Bool(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Float4(l, lb), ColumnValues::Float4(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Float8(l, lb), ColumnValues::Float8(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Int1(l, lb), ColumnValues::Int1(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Int2(l, lb), ColumnValues::Int2(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Int4(l, lb), ColumnValues::Int4(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Int8(l, lb), ColumnValues::Int8(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Int16(l, lb), ColumnValues::Int16(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Utf8(l, lb), ColumnValues::Utf8(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Uint1(l, lb), ColumnValues::Uint1(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Uint2(l, lb), ColumnValues::Uint2(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Uint4(l, lb), ColumnValues::Uint4(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Uint8(l, lb), ColumnValues::Uint8(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Uint16(l, lb), ColumnValues::Uint16(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Date(l, lb), ColumnValues::Date(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::DateTime(l, lb), ColumnValues::DateTime(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Time(l, lb), ColumnValues::Time(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Interval(l, lb), ColumnValues::Interval(r, rb)) => {
                l.extend(r);
                lb.extend(&rb);
            }

            (ColumnValues::Undefined(l_len), ColumnValues::Undefined(r_len)) => {
                *l_len += r_len;
            }

            // Promote Undefined
            (ColumnValues::Undefined(l_len), typed_lr) => match typed_lr {
                ColumnValues::Bool(r, rb) => {
                    let mut values = CowVec::new(vec![false; *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::bool_with_bitvec(values, bitvec);
                }
                ColumnValues::Float4(r, rb) => {
                    let mut values = CowVec::new(vec![0.0f32; *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::float4_with_bitvec(values, bitvec);
                }
                ColumnValues::Float8(r, rb) => {
                    let mut values = CowVec::new(vec![0.0f64; *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::float8_with_bitvec(values, bitvec);
                }
                ColumnValues::Int1(r, rb) => {
                    let mut values = CowVec::new(vec![0i8; *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::int1_with_bitvec(values, bitvec);
                }
                ColumnValues::Int2(r, rb) => {
                    let mut values = CowVec::new(vec![0i16; *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::int2_with_bitvec(values, bitvec);
                }
                ColumnValues::Int4(r, rb) => {
                    let mut values = CowVec::new(vec![0i32; *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::int4_with_bitvec(values, bitvec);
                }
                ColumnValues::Int8(r, rb) => {
                    let mut values = CowVec::new(vec![0i64; *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::int8_with_bitvec(values, bitvec);
                }
                ColumnValues::Int16(r, rb) => {
                    let mut values = CowVec::new(vec![0i128; *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::int16_with_bitvec(values, bitvec);
                }
                ColumnValues::Utf8(r, rb) => {
                    let mut values = CowVec::new(vec!["".to_string(); *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::utf8_with_bitvec(values, bitvec);
                }
                ColumnValues::Uint1(r, rb) => {
                    let mut values = CowVec::new(vec![0u8; *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::uint1_with_bitvec(values, bitvec);
                }
                ColumnValues::Uint2(r, rb) => {
                    let mut values = CowVec::new(vec![0u16; *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::uint2_with_bitvec(values, bitvec);
                }
                ColumnValues::Uint4(r, rb) => {
                    let mut values = CowVec::new(vec![0u32; *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::uint4_with_bitvec(values, bitvec);
                }
                ColumnValues::Uint8(r, rb) => {
                    let mut values = CowVec::new(vec![0u64; *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::uint8_with_bitvec(values, bitvec);
                }
                ColumnValues::Uint16(r, rb) => {
                    let mut values = CowVec::new(vec![0u128; *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::uint16_with_bitvec(values, bitvec);
                }
                ColumnValues::Date(r, rb) => {
                    let mut values = CowVec::new(vec![Date::default(); *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::date_with_bitvec(values, bitvec);
                }
                ColumnValues::DateTime(r, rb) => {
                    let mut values = CowVec::new(vec![DateTime::default(); *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::datetime_with_bitvec(values, bitvec);
                }
                ColumnValues::Time(r, rb) => {
                    let mut values = CowVec::new(vec![Time::default(); *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::time_with_bitvec(values, bitvec);
                }
                ColumnValues::Interval(r, rb) => {
                    let mut values = CowVec::new(vec![Interval::default(); *l_len]);
                    values.extend(r);

                    let mut bitvec = BitVec::new(*l_len, false);
                    bitvec.extend(&rb);

                    *self = ColumnValues::interval_with_bitvec(values, bitvec);
                }
                ColumnValues::Undefined(_) => {}
            },

            // Prevent appending typed into Undefined
            (typed_l, ColumnValues::Undefined(r_len)) => match typed_l {
                ColumnValues::Bool(l, lb) => {
                    l.extend(std::iter::repeat(false).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Float4(l, lb) => {
                    l.extend(std::iter::repeat(0.0f32).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Float8(l, lb) => {
                    l.extend(std::iter::repeat(0.0f64).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Int1(l, lb) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Int2(l, lb) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Int4(l, lb) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Int8(l, lb) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Int16(l, lb) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Utf8(l, lb) => {
                    l.extend(std::iter::repeat(String::new()).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Uint1(l, lb) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Uint2(l, lb) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Uint4(l, lb) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Uint8(l, lb) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Uint16(l, lb) => {
                    l.extend(std::iter::repeat(0).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Date(l, lb) => {
                    l.extend(std::iter::repeat(Date::default()).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::DateTime(l, lb) => {
                    l.extend(std::iter::repeat(DateTime::default()).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Time(l, lb) => {
                    l.extend(std::iter::repeat(Time::default()).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
                }
                ColumnValues::Interval(l, lb) => {
                    l.extend(std::iter::repeat(Interval::default()).take(r_len));
                    lb.extend(&std::iter::repeat(false).take(r_len).collect::<Vec<_>>().into());
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
