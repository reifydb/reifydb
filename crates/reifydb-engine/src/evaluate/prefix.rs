// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	err,
	interface::evaluate::expression::{PrefixExpression, PrefixOperator},
	result::error::diagnostic::operator,
};

use crate::{
	columnar::{Column, ColumnData, ColumnQualified, SourceQualified},
	evaluate::{EvaluationContext, StandardEvaluator, evaluate},
};

impl StandardEvaluator {
	pub(crate) fn prefix(
		&self,
		ctx: &EvaluationContext,
		prefix: &PrefixExpression,
	) -> crate::Result<Column> {
		let column = evaluate(ctx, &prefix.expression)?;

		match column.data() {
            // EngineColumnData::Bool(_, _) => Err("Cannot apply prefix operator to bool".into()),
            ColumnData::Bool(container) => match prefix.operator {
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
                        Some(table) => Column::SourceQualified(SourceQualified {
                            source: table.to_string(),
                            name: column.name().to_string(),
                            data: ColumnData::bool_with_bitvec(result, container.bitvec()),
                        }),
                        None => Column::ColumnQualified(ColumnQualified {
                            name: column.name().to_string(),
                            data: ColumnData::bool_with_bitvec(result, container.bitvec()),
                        }),
                    })
                }
                _ => err!(reifydb_core::error::diagnostic::engine::frame_error(
                    "Cannot apply arithmetic prefix operator to bool".to_string()
                )),
            },

            ColumnData::Float4(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.fragment()
                                ));
                            }
                        });
                    } else {
                        result.push(0.0f32);
                    }
                }
                Ok(match column.table() {
                    Some(table) => Column::SourceQualified(SourceQualified {
                        source: table.to_string(),
                        name: column.name().to_string(),
                        data: ColumnData::float4_with_bitvec(result, container.bitvec()),
                    }),
                    None => Column::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: ColumnData::float4_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnData::Float8(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.fragment()
                                ));
                            }
                        });
                    } else {
                        result.push(0.0f64);
                    }
                }
                Ok(match column.table() {
                    Some(table) => Column::SourceQualified(SourceQualified {
                        source: table.to_string(),
                        name: column.name().to_string(),
                        data: ColumnData::float8_with_bitvec(result, container.bitvec()),
                    }),
                    None => Column::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: ColumnData::float8_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnData::Int1(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.fragment()
                                ));
                            }
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => Column::SourceQualified(SourceQualified {
                        source: table.to_string(),
                        name: column.name().to_string(),
                        data: ColumnData::int1_with_bitvec(result, container.bitvec()),
                    }),
                    None => Column::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: ColumnData::int1_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnData::Int2(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.fragment()
                                ));
                            }
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => Column::SourceQualified(SourceQualified {
                        source: table.to_string(),
                        name: column.name().to_string(),
                        data: ColumnData::int2_with_bitvec(result, container.bitvec()),
                    }),
                    None => Column::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: ColumnData::int2_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnData::Int4(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.fragment()
                                ));
                            }
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => Column::SourceQualified(SourceQualified {
                        source: table.to_string(),
                        name: column.name().to_string(),
                        data: ColumnData::int4_with_bitvec(result, container.bitvec()),
                    }),
                    None => Column::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: ColumnData::int4_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnData::Int8(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.fragment()
                                ));
                            }
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => Column::SourceQualified(SourceQualified {
                        source: table.to_string(),
                        name: column.name().to_string(),
                        data: ColumnData::int8_with_bitvec(result, container.bitvec()),
                    }),
                    None => Column::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: ColumnData::int8_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnData::Int16(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for (idx, val) in container.data().iter().enumerate() {
                    if container.is_defined(idx) {
                        result.push(match prefix.operator {
                            PrefixOperator::Minus(_) => -*val,
                            PrefixOperator::Plus(_) => *val,
                            PrefixOperator::Not(_) => {
                                return err!(operator::not_can_not_applied_to_number(
                                    prefix.fragment()
                                ));
                            }
                        });
                    } else {
                        result.push(0);
                    }
                }
                Ok(match column.table() {
                    Some(table) => Column::SourceQualified(SourceQualified {
                        source: table.to_string(),
                        name: column.name().to_string(),
                        data: ColumnData::int16_with_bitvec(result, container.bitvec()),
                    }),
                    None => Column::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: ColumnData::int16_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnData::Utf8(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_text(prefix.fragment()))
                }
                _ => err!(reifydb_core::error::diagnostic::engine::frame_error(
                    "Cannot apply arithmetic prefix operator to text".to_string()
                )),
            },

            ColumnData::Uint1(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for val in container.data().iter() {
                    let signed = *val as i8;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                        PrefixOperator::Not(_) => {
                            return err!(operator::not_can_not_applied_to_number(prefix.fragment()));
                        }
                    });
                }
                Ok(match column.table() {
                    Some(table) => Column::SourceQualified(SourceQualified {
                        source: table.to_string(),
                        name: column.name().to_string(),
                        data: ColumnData::int1_with_bitvec(result, container.bitvec()),
                    }),
                    None => Column::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: ColumnData::int1_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnData::Uint2(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for val in container.data().iter() {
                    let signed = *val as i16;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                        PrefixOperator::Not(_) => {
                            return err!(operator::not_can_not_applied_to_number(prefix.fragment()));
                        }
                    });
                }
                Ok(match column.table() {
                    Some(table) => Column::SourceQualified(SourceQualified {
                        source: table.to_string(),
                        name: column.name().to_string(),
                        data: ColumnData::int2_with_bitvec(result, container.bitvec()),
                    }),
                    None => Column::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: ColumnData::int2_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnData::Uint4(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for val in container.data().iter() {
                    let signed = *val as i32;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                        PrefixOperator::Not(_) => {
                            return err!(operator::not_can_not_applied_to_number(prefix.fragment()));
                        }
                    });
                }
                Ok(match column.table() {
                    Some(table) => Column::SourceQualified(SourceQualified {
                        source: table.to_string(),
                        name: column.name().to_string(),
                        data: ColumnData::int4_with_bitvec(result, container.bitvec()),
                    }),
                    None => Column::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: ColumnData::int4_with_bitvec(result, container.bitvec()),
                    }),
                })
            }

            ColumnData::Uint8(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for val in container.data().iter() {
                    let signed = *val as i64;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                        PrefixOperator::Not(_) => {
                            return err!(operator::not_can_not_applied_to_number(prefix.fragment()));
                        }
                    });
                }
                Ok(match column.table() {
                    Some(table) => Column::SourceQualified(SourceQualified {
                        source: table.to_string(),
                        name: column.name().to_string(),
                        data: ColumnData::int8_with_bitvec(result, container.bitvec()),
                    }),
                    None => Column::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: ColumnData::int8_with_bitvec(result, container.bitvec()),
                    }),
                })
            }
            ColumnData::Uint16(container) => {
                let mut result = Vec::with_capacity(container.data().len());
                for val in container.data().iter() {
                    let signed = *val as i128;
                    result.push(match prefix.operator {
                        PrefixOperator::Minus(_) => -signed,
                        PrefixOperator::Plus(_) => signed,
                        PrefixOperator::Not(_) => {
                            return err!(operator::not_can_not_applied_to_number(prefix.fragment()));
                        }
                    });
                }
                Ok(match column.table() {
                    Some(table) => Column::SourceQualified(SourceQualified {
                        source: table.to_string(),
                        name: column.name().to_string(),
                        data: ColumnData::int16_with_bitvec(result, container.bitvec()),
                    }),
                    None => Column::ColumnQualified(ColumnQualified {
                        name: column.name().to_string(),
                        data: ColumnData::int16_with_bitvec(result, container.bitvec()),
                    }),
                })
            }
            // EngineColumnData::Undefined(_) => {
            //     Err("Cannot apply prefix operator to undefined data".into())
            // }
            ColumnData::Undefined(_) => {
                unimplemented!()
            }

            ColumnData::Date(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_temporal(prefix.fragment()))
                }
                _ => unimplemented!(),
            },
            ColumnData::DateTime(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_temporal(prefix.fragment()))
                }
                _ => unimplemented!(),
            },
            ColumnData::Time(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_temporal(prefix.fragment()))
                }
                _ => unimplemented!(),
            },
            ColumnData::Interval(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_temporal(prefix.fragment()))
                }
                _ => unimplemented!(),
            },
            ColumnData::RowNumber(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_number(prefix.fragment()))
                }
                _ => unimplemented!(),
            },
            ColumnData::IdentityId(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_uuid(prefix.fragment()))
                }
                _ => unimplemented!(),
            },
            ColumnData::Uuid4(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_uuid(prefix.fragment()))
                }
                _ => unimplemented!(),
            },
            ColumnData::Uuid7(_) => match prefix.operator {
                PrefixOperator::Not(_) => {
                    err!(operator::not_can_not_applied_to_uuid(prefix.fragment()))
                }
                _ => unimplemented!(),
            },
            ColumnData::Blob(_) => match prefix.operator {
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
