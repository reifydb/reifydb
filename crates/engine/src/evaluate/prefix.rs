// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{evaluate, EvaluationContext, Evaluator};
use reifydb_core::frame::{ColumnQualified, ColumnValues, FrameColumn, TableQualified};
use reifydb_rql::expression::{PrefixExpression, PrefixOperator};

impl Evaluator {
    pub(crate) fn prefix(
        &mut self,
        prefix: &PrefixExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let column = evaluate(&prefix.expression, ctx)?;

        match column.values() {
            // ColumnValues::Bool(_, _) => Err("Cannot apply prefix operator to bool".into()),
            ColumnValues::Bool(_, _) => unimplemented!(),

            ColumnValues::Float4(values, bitvec) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if bitvec.get(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0.0f32);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::float4_with_bitvec(result, bitvec),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::float4_with_bitvec(result, bitvec),
                    }),
                })
            }

            ColumnValues::Float8(values, bitvec) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if bitvec.get(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0.0f64);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::float8_with_bitvec(result, bitvec),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::float8_with_bitvec(result, bitvec),
                    }),
                })
            }

            ColumnValues::Int1(values, bitvec) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if bitvec.get(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int1_with_bitvec(result, bitvec),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int1_with_bitvec(result, bitvec),
                    }),
                })
            }

            ColumnValues::Int2(values, bitvec) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if bitvec.get(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int2_with_bitvec(result, bitvec),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int2_with_bitvec(result, bitvec),
                    }),
                })
            }

            ColumnValues::Int4(values, bitvec) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if bitvec.get(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int4_with_bitvec(result, bitvec),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int4_with_bitvec(result, bitvec),
                    }),
                })
            }

            ColumnValues::Int8(values, bitvec) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if bitvec.get(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int8_with_bitvec(result, bitvec),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int8_with_bitvec(result, bitvec),
                    }),
                })
            }

            ColumnValues::Int16(values, bitvec) => {
                let mut result = Vec::with_capacity(values.len());
                for (idx, val) in values.iter().enumerate() {
                    if bitvec.get(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int16_with_bitvec(result, bitvec),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int16_with_bitvec(result, bitvec),
                    }),
                })
            }

            // ColumnValues::String(_, _) => Err("Cannot apply prefix operator to string".into()),
            ColumnValues::Utf8(_, _) => unimplemented!(),

            ColumnValues::Uint1(values, bitvec) => {
                let mut result = Vec::with_capacity(values.len());
                for val in values.iter() {
                    let signed = *val as i8;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                    });
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int1_with_bitvec(result, bitvec),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int1_with_bitvec(result, bitvec),
                    }),
                })
            }

            ColumnValues::Uint2(values, bitvec) => {
                let mut result = Vec::with_capacity(values.len());
                for val in values.iter() {
                    let signed = *val as i16;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                    });
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int2_with_bitvec(result, bitvec),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int2_with_bitvec(result, bitvec),
                    }),
                })
            }

            ColumnValues::Uint4(values, bitvec) => {
                let mut result = Vec::with_capacity(values.len());
                for val in values.iter() {
                    let signed = *val as i32;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                    });
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int4_with_bitvec(result, bitvec),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int4_with_bitvec(result, bitvec),
                    }),
                })
            }

            ColumnValues::Uint8(values, bitvec) => {
                let mut result = Vec::with_capacity(values.len());
                for val in values.iter() {
                    let signed = *val as i64;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                    });
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int8_with_bitvec(result, bitvec),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int8_with_bitvec(result, bitvec),
                    }),
                })
            }
            ColumnValues::Uint16(values, bitvec) => {
                let mut result = Vec::with_capacity(values.len());
                for val in values.iter() {
                    let signed = *val as i128;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                    });
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int16_with_bitvec(result, bitvec),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int16_with_bitvec(result, bitvec),
                    }),
                })
            }
            // ColumnValues::Undefined(_) => {
            //     Err("Cannot apply prefix operator to undefined values".into())
            // }
            ColumnValues::Undefined(_) => {
                unimplemented!()
            }

            ColumnValues::Date(_, _) => unimplemented!(),
            ColumnValues::DateTime(_, _) => unimplemented!(),
            ColumnValues::Time(_, _) => unimplemented!(),
            ColumnValues::Interval(_, _) => unimplemented!(),
            ColumnValues::RowId(_, _) => unimplemented!(),
            ColumnValues::Uuid4(_, _) => unimplemented!(),
            ColumnValues::Uuid7(_, _) => unimplemented!(),
        }
    }
}
