// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator, evaluate};
use reifydb_core::err;
use reifydb_core::error::diagnostic::operator;
use reifydb_core::frame::{ColumnQualified, ColumnValues, FrameColumn, TableQualified};
use reifydb_core::expression::{PrefixExpression, PrefixOperator};

impl Evaluator {
    pub(crate) fn prefix(
        &mut self,
        prefix: &PrefixExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<FrameColumn> {
        let column = evaluate(&prefix.expression, ctx)?;

        match column.values() {
            // ColumnValues::Bool(_, _) => Err("Cannot apply prefix operator to bool".into()),
            ColumnValues::Bool(container) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    let mut result = Vec::with_capacity(container.values().len());
                    for (idx, val) in container.values().iter().enumerate() {
                        if container.is_defined(idx) {
                            result.push(!val);
                        } else {
                            result.push(false);
                        }
                    }
                    Ok(match column.table() {
                        Some(table) => FrameColumn::TableQualified(TableQualified {
                            table: table.to_string(),
                            name: column.name().to_string(),
                            values: ColumnValues::bool_with_bitvec(result, container.bitvec()),
                        }),
                        None => FrameColumn::ColumnQualified(ColumnQualified {
                            name: column.name().to_string(),
                            values: ColumnValues::bool_with_bitvec(result, container.bitvec()),
                        }),
                    })
                }
                _ => err!(reifydb_core::error::diagnostic::engine::frame_error(
                    "Cannot apply arithmetic prefix operator to bool".to_string()
                )),
            },

            ColumnValues::Float4(container) => {
                let mut result = Vec::with_capacity(container.values().len());
                for (idx, val) in container.values().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.span()
                                ));
                            }
                        });
                    } else {
                        result.push(0.0f32);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::float4_with_bitvec(result, container.bitvec()),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::float4_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnValues::Float8(container) => {
                let mut result = Vec::with_capacity(container.values().len());
                for (idx, val) in container.values().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.span()
                                ));
                            }
                        });
                    } else {
                        result.push(0.0f64);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::float8_with_bitvec(result, container.bitvec()),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::float8_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnValues::Int1(container) => {
                let mut result = Vec::with_capacity(container.values().len());
                for (idx, val) in container.values().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.span()
                                ));
                            }
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int1_with_bitvec(result, container.bitvec()),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int1_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnValues::Int2(container) => {
                let mut result = Vec::with_capacity(container.values().len());
                for (idx, val) in container.values().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.span()
                                ));
                            }
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int2_with_bitvec(result, container.bitvec()),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int2_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnValues::Int4(container) => {
                let mut result = Vec::with_capacity(container.values().len());
                for (idx, val) in container.values().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.span()
                                ));
                            }
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int4_with_bitvec(result, container.bitvec()),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int4_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnValues::Int8(container) => {
                let mut result = Vec::with_capacity(container.values().len());
                for (idx, val) in container.values().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.span()
                                ));
                            }
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int8_with_bitvec(result, container.bitvec()),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int8_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnValues::Int16(container) => {
                let mut result = Vec::with_capacity(container.values().len());
                for (idx, val) in container.values().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.span()
                                ));
                            }
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int16_with_bitvec(result, container.bitvec()),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int16_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnValues::Utf8(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_text(prefix.span()))
                }
                _ => err!(reifydb_core::error::diagnostic::engine::frame_error(
                    "Cannot apply arithmetic prefix operator to text".to_string()
                )),
            },

            ColumnValues::Uint1(container) => {
                let mut result = Vec::with_capacity(container.values().len());
                for val in container.values().iter() {
                    let signed = *val as i8;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                        PrefixOperator::Not(_) => {
                            return err!(operator::not_can_not_applied_to_number(prefix.span()));
                        }
                    });
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int1_with_bitvec(result, container.bitvec()),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int1_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnValues::Uint2(container) => {
                let mut result = Vec::with_capacity(container.values().len());
                for val in container.values().iter() {
                    let signed = *val as i16;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                        PrefixOperator::Not(_) => {
                            return err!(operator::not_can_not_applied_to_number(prefix.span()));
                        }
                    });
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int2_with_bitvec(result, container.bitvec()),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int2_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnValues::Uint4(container) => {
                let mut result = Vec::with_capacity(container.values().len());
                for val in container.values().iter() {
                    let signed = *val as i32;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                        PrefixOperator::Not(_) => {
                            return err!(operator::not_can_not_applied_to_number(prefix.span()));
                        }
                    });
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int4_with_bitvec(result, container.bitvec()),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int4_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnValues::Uint8(container) => {
                let mut result = Vec::with_capacity(container.values().len());
                for val in container.values().iter() {
                    let signed = *val as i64;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                        PrefixOperator::Not(_) => {
                            return err!(operator::not_can_not_applied_to_number(prefix.span()));
                        }
                    });
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int8_with_bitvec(result, container.bitvec()),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int8_with_bitvec(result, container.bitvec()),
                    }),
                })
            }
            ColumnValues::Uint16(container) => {
                let mut result = Vec::with_capacity(container.values().len());
                for val in container.values().iter() {
                    let signed = *val as i128;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                        PrefixOperator::Not(_) => {
                            return err!(operator::not_can_not_applied_to_number(prefix.span()));
                        }
                    });
                }
                Ok(match column.table() {
                    Some(table) => FrameColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        values: ColumnValues::int16_with_bitvec(result, container.bitvec()),
                    }),
                    None => FrameColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        values: ColumnValues::int16_with_bitvec(result, container.bitvec()),
                    }),
                })
            }
            // ColumnValues::Undefined(_) => {
            //     Err("Cannot apply prefix operator to undefined values".into())
            // }
            ColumnValues::Undefined(_) => {
                unimplemented!()
            }

            ColumnValues::Date(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_temporal(prefix.span()))
                }
                _ => unimplemented!(),
            },
            ColumnValues::DateTime(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_temporal(prefix.span()))
                }
                _ => unimplemented!(),
            },
            ColumnValues::Time(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_temporal(prefix.span()))
                }
                _ => unimplemented!(),
            },
            ColumnValues::Interval(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_temporal(prefix.span()))
                }
                _ => unimplemented!(),
            },
            ColumnValues::RowId(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_number(prefix.span()))
                }
                _ => unimplemented!(),
            },
            ColumnValues::Uuid4(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_uuid(prefix.span()))
                }
                _ => unimplemented!(),
            },
            ColumnValues::Uuid7(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_uuid(prefix.span()))
                }
                _ => unimplemented!(),
            },
            ColumnValues::Blob(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(reifydb_core::error::diagnostic::engine::frame_error(
                        "Cannot apply NOT operator to BLOB".to_string()
                    ))
                }
                _ => err!(reifydb_core::error::diagnostic::engine::frame_error(
                    "Cannot apply arithmetic prefix operator to BLOB".to_string()
                )),
            },
        }
    }
}
