// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::Value;
use crate::ColumnValues;

impl ColumnValues {
    pub fn push_i16(&mut self, value: i16) {
        match self {
            ColumnValues::Int2(values, validity) => {
                values.push(value);
                validity.push(true);
            }
            _ => unreachable!(),
        }
    }

    pub fn push_value(&mut self, value: Value) {
        match self {
            ColumnValues::Bool(values, validity) => match value {
                Value::Bool(v) => {
                    values.push(v);
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Float4(values, validity) => match value {
                Value::Float4(v) => {
                    values.push(v.value());
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Float8(values, validity) => match value {
                Value::Float8(v) => {
                    values.push(v.value());
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Int1(values, validity) => match value {
                Value::Int1(v) => {
                    values.push(v);
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Int2(values, validity) => match value {
                Value::Int2(v) => {
                    values.push(v);
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Int4(values, validity) => match value {
                Value::Int4(v) => {
                    values.push(v);
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Int8(values, validity) => match value {
                Value::Int8(v) => {
                    values.push(v);
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Int16(values, validity) => match value {
                Value::Int16(v) => {
                    values.push(v);
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::String(values, validity) => match value {
                Value::String(v) => {
                    values.push(v);
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Uint1(values, validity) => match value {
                Value::Uint1(v) => {
                    values.push(v);
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Uint2(values, validity) => match value {
                Value::Uint2(v) => {
                    values.push(v);
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Uint4(values, validity) => match value {
                Value::Uint4(v) => {
                    values.push(v);
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Uint8(values, validity) => match value {
                Value::Uint8(v) => {
                    values.push(v);
                    validity.push(true);
                }
                _ => unimplemented!(),
            },
            ColumnValues::Uint16(values, validity) => match value {
                Value::Uint16(v) => {
                    values.push(v);
                    validity.push(true);
                }
                _ => unimplemented!(),
            },

            ColumnValues::Undefined(len) => {
                let mut validity = vec![false; *len];
                validity.push(true);

                *self = match value {
                    Value::Bool(v) => ColumnValues::bool_with_validity([v], validity),
                    Value::Float4(v) => ColumnValues::float4_with_validity([v.value()], validity),
                    Value::Float8(v) => ColumnValues::float8_with_validity([v.value()], validity),
                    Value::Int1(v) => ColumnValues::int1_with_validity([v], validity),
                    Value::Int2(v) => ColumnValues::int2_with_validity([v], validity),
                    Value::Int4(v) => ColumnValues::int4_with_validity([v], validity),
                    Value::Int8(v) => ColumnValues::int8_with_validity([v], validity),
                    Value::Int16(v) => ColumnValues::int16_with_validity([v], validity),
                    Value::Uint1(v) => ColumnValues::uint1_with_validity([v], validity),
                    Value::Uint2(v) => ColumnValues::uint2_with_validity([v], validity),
                    Value::Uint4(v) => ColumnValues::uint4_with_validity([v], validity),
                    Value::Uint8(v) => ColumnValues::uint8_with_validity([v], validity),
                    Value::Uint16(v) => ColumnValues::uint16_with_validity([v], validity),
                    Value::String(v) => ColumnValues::string_with_validity([v], validity),
                    _ => unimplemented!(),
                };
            }
        }
    }
}
