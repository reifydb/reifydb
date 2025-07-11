// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::{Context, Evaluator, evaluate};
use crate::frame::{Column, ColumnValues};
use reifydb_rql::expression::{PrefixExpression, PrefixOperator};

impl Evaluator {
    pub(crate) fn prefix(
        &mut self,
        prefix: &PrefixExpression,
        ctx: &Context,
    ) -> evaluate::Result<Column> {
        let column = evaluate(&prefix.expression, ctx)?;

        match column.values {
            // ColumnValues::Bool(_, _) => Err("Cannot apply prefix operator to bool".into()),
            ColumnValues::Bool(_, _) => unimplemented!(),

            ColumnValues::Float4(values, valid) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if valid.get(idx).copied().unwrap_or(false) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0.0f32);
                    }
                }
                Ok(Column {
                    name: column.name,
                    values: ColumnValues::float4_with_validity(result, valid),
                })
            }

            ColumnValues::Float8(values, valid) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if valid.get(idx).copied().unwrap_or(false) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0.0f64);
                    }
                }
                Ok(Column {
                    name: column.name,
                    values: ColumnValues::float8_with_validity(result, valid),
                })
            }

            ColumnValues::Int1(values, valid) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if valid.get(idx).copied().unwrap_or(false) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(Column {
                    name: column.name,
                    values: ColumnValues::int1_with_validity(result, valid),
                })
            }

            ColumnValues::Int2(values, valid) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if valid.get(idx).copied().unwrap_or(false) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(Column {
                    name: column.name,
                    values: ColumnValues::int2_with_validity(result, valid),
                })
            }

            ColumnValues::Int4(values, valid) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if valid.get(idx).copied().unwrap_or(false) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(Column {
                    name: column.name,
                    values: ColumnValues::int4_with_validity(result, valid),
                })
            }

            ColumnValues::Int8(values, valid) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if valid.get(idx).copied().unwrap_or(false) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(Column {
                    name: column.name,
                    values: ColumnValues::int8_with_validity(result, valid),
                })
            }

            ColumnValues::Int16(values, valid) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if valid.get(idx).copied().unwrap_or(false) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(Column {
                    name: column.name,
                    values: ColumnValues::int16_with_validity(result, valid),
                })
            }

            // ColumnValues::String(_, _) => Err("Cannot apply prefix operator to string".into()),
            ColumnValues::Utf8(_, _) => unimplemented!(),

            ColumnValues::Uint1(values, valid) => {
                let mut result = Vec::with_capacity(values.len());
                for val in values {
                    let signed = val as i8;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                    });
                }
                Ok(Column {
                    name: column.name,
                    values: ColumnValues::int1_with_validity(result, valid),
                })
            }

            ColumnValues::Uint2(values, valid) => {
                let mut result = Vec::with_capacity(values.len());
                for val in values {
                    let signed = val as i16;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                    });
                }
                Ok(Column {
                    name: column.name,
                    values: ColumnValues::int2_with_validity(result, valid),
                })
            }

            ColumnValues::Uint4(values, valid) => {
                let mut result = Vec::with_capacity(values.len());
                for val in values {
                    let signed = val as i32;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                    });
                }
                Ok(Column {
                    name: column.name,
                    values: ColumnValues::int4_with_validity(result, valid),
                })
            }

            ColumnValues::Uint8(values, valid) => {
                let mut result = Vec::with_capacity(values.len());
                for val in values {
                    let signed = val as i64;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                    });
                }
                Ok(Column {
                    name: column.name,
                    values: ColumnValues::int8_with_validity(result, valid),
                })
            }
            ColumnValues::Uint16(values, valid) => {
                let mut result = Vec::with_capacity(values.len());
                for val in values {
                    let signed = val as i128;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                    });
                }
                Ok(Column {
                    name: column.name,
                    values: ColumnValues::int16_with_validity(result, valid),
                })
            }
            // ColumnValues::Undefined(_) => {
            //     Err("Cannot apply prefix operator to undefined values".into())
            // }
            ColumnValues::Undefined(_) => {
                unimplemented!()
            }
        }
    }
}
