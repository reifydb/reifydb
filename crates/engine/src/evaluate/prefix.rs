// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::evaluate::{EvaluationContext, Evaluator, evaluate};
use reifydb_core::err;
use reifydb_core::error::diagnostic::operator;
use crate::column::{ColumnQualified, EngineColumnData, EngineColumn, TableQualified};
use reifydb_rql::expression::{PrefixExpression, PrefixOperator};

impl Evaluator {
    pub(crate) fn prefix(
        &mut self,
        prefix: &PrefixExpression,
        ctx: &EvaluationContext,
    ) -> crate::Result<EngineColumn> {
        let column = evaluate(&prefix.expression, ctx)?;

        match column.data() {
            // EngineColumnData::Bool(_, _) => Err("Cannot apply prefix operator to bool".into()),
            EngineColumnData::Bool(container) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    let mut result = Vec::with_capacity(container.data().len());
                    for (idx, val) in container.data().iter().enumerate() {
                        if container.is_defined(idx) {
                            result.push(!val);
                        } else {
                            result.push(false);
                        }
                    }
                    Ok(match column.table() {
                        Some(table) => EngineColumn::TableQualified(TableQualified {
                            table: table.to_string(),
                            name: column.name().to_string(),
                            data: EngineColumnData::bool_with_bitvec(result, container.bitvec()),
                        }),
                        None => EngineColumn::ColumnQualified(ColumnQualified {
                            name: column.name().to_string(),
                            data: EngineColumnData::bool_with_bitvec(result, container.bitvec()),
                        }),
                    })
                }
                _ => err!(reifydb_core::error::diagnostic::engine::frame_error(
                    "Cannot apply arithmetic prefix operator to bool".to_string()
                )),
            },

            EngineColumnData::Float4(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
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
                    Some(table) => EngineColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        data: EngineColumnData::float4_with_bitvec(result, container.bitvec()),
                    }),
                    None => EngineColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: EngineColumnData::float4_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            EngineColumnData::Float8(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
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
                    Some(table) => EngineColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        data: EngineColumnData::float8_with_bitvec(result, container.bitvec()),
                    }),
                    None => EngineColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: EngineColumnData::float8_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            EngineColumnData::Int1(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
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
                    Some(table) => EngineColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        data: EngineColumnData::int1_with_bitvec(result, container.bitvec()),
                    }),
                    None => EngineColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: EngineColumnData::int1_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            EngineColumnData::Int2(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
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
                    Some(table) => EngineColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        data: EngineColumnData::int2_with_bitvec(result, container.bitvec()),
                    }),
                    None => EngineColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: EngineColumnData::int2_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            EngineColumnData::Int4(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
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
                    Some(table) => EngineColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        data: EngineColumnData::int4_with_bitvec(result, container.bitvec()),
                    }),
                    None => EngineColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: EngineColumnData::int4_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            EngineColumnData::Int8(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
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
                    Some(table) => EngineColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        data: EngineColumnData::int8_with_bitvec(result, container.bitvec()),
                    }),
                    None => EngineColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: EngineColumnData::int8_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            EngineColumnData::Int16(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
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
                    Some(table) => EngineColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        data: EngineColumnData::int16_with_bitvec(result, container.bitvec()),
                    }),
                    None => EngineColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: EngineColumnData::int16_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            EngineColumnData::Utf8(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_text(prefix.span()))
                }
                _ => err!(reifydb_core::error::diagnostic::engine::frame_error(
                    "Cannot apply arithmetic prefix operator to text".to_string()
                )),
            },

            EngineColumnData::Uint1(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for val in container.data().iter() {
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
                    Some(table) => EngineColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        data: EngineColumnData::int1_with_bitvec(result, container.bitvec()),
                    }),
                    None => EngineColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: EngineColumnData::int1_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            EngineColumnData::Uint2(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for val in container.data().iter() {
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
                    Some(table) => EngineColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        data: EngineColumnData::int2_with_bitvec(result, container.bitvec()),
                    }),
                    None => EngineColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: EngineColumnData::int2_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            EngineColumnData::Uint4(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for val in container.data().iter() {
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
                    Some(table) => EngineColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        data: EngineColumnData::int4_with_bitvec(result, container.bitvec()),
                    }),
                    None => EngineColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: EngineColumnData::int4_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            EngineColumnData::Uint8(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for val in container.data().iter() {
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
                    Some(table) => EngineColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        data: EngineColumnData::int8_with_bitvec(result, container.bitvec()),
                    }),
                    None => EngineColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: EngineColumnData::int8_with_bitvec(result, container.bitvec()),
                    }),
                })
            }
            EngineColumnData::Uint16(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for val in container.data().iter() {
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
                    Some(table) => EngineColumn::TableQualified(TableQualified {
                        table: table.to_string(),
                        name: column.name().to_string(),
                        data: EngineColumnData::int16_with_bitvec(result, container.bitvec()),
                    }),
                    None => EngineColumn::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: EngineColumnData::int16_with_bitvec(result, container.bitvec()),
                    }),
                })
            }
            // EngineColumnData::Undefined(_) => {
            //     Err("Cannot apply prefix operator to undefined data".into())
            // }
            EngineColumnData::Undefined(_) => {
                unimplemented!()
            }

            EngineColumnData::Date(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_temporal(prefix.span()))
                }
                _ => unimplemented!(),
            },
            EngineColumnData::DateTime(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_temporal(prefix.span()))
                }
                _ => unimplemented!(),
            },
            EngineColumnData::Time(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_temporal(prefix.span()))
                }
                _ => unimplemented!(),
            },
            EngineColumnData::Interval(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_temporal(prefix.span()))
                }
                _ => unimplemented!(),
            },
            EngineColumnData::RowId(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_number(prefix.span()))
                }
                _ => unimplemented!(),
            },
            EngineColumnData::Uuid4(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_uuid(prefix.span()))
                }
                _ => unimplemented!(),
            },
            EngineColumnData::Uuid7(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_uuid(prefix.span()))
                }
                _ => unimplemented!(),
            },
            EngineColumnData::Blob(_) => match prefix.operator {
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
