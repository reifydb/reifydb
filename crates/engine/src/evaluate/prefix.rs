// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use operator::{not_can_not_applied_to_number, not_can_not_applied_to_temporal, not_can_not_applied_to_uuid};
use reifydb_core::{
	err,
	interface::evaluate::expression::{PrefixExpression, PrefixOperator},
	value::columnar::{Column, ColumnData, ColumnQualified, SourceQualified},
};
use reifydb_type::{
	Decimal, Int, Uint, diagnostic,
	diagnostic::{engine::frame_error, operator},
};

use crate::evaluate::{EvaluationContext, StandardEvaluator, evaluate};

impl StandardEvaluator {
	pub(crate) fn prefix<'a>(
		&self,
		ctx: &EvaluationContext<'a>,
		prefix: &PrefixExpression<'a>,
	) -> crate::Result<Column<'a>> {
		let column = evaluate(ctx, &prefix.expression)?;

		match column.data() {
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
							source: table.clone(),
							name: column.name().clone(),
							data: ColumnData::bool_with_bitvec(result, container.bitvec()),
						}),
						None => Column::ColumnQualified(ColumnQualified {
							name: column.name().clone(),
							data: ColumnData::bool_with_bitvec(result, container.bitvec()),
						}),
					})
				}
				_ => err!(diagnostic::engine::frame_error(
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
								return err!(not_can_not_applied_to_number(
									prefix.full_fragment_owned()
								));
							}
						});
					} else {
						result.push(0.0f32);
					}
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::float4_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
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
								return err!(not_can_not_applied_to_number(
									prefix.full_fragment_owned()
								));
							}
						});
					} else {
						result.push(0.0f64);
					}
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::float8_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
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
								return err!(not_can_not_applied_to_number(
									prefix.full_fragment_owned()
								));
							}
						});
					} else {
						result.push(0);
					}
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::int1_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
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
								return err!(not_can_not_applied_to_number(
									prefix.full_fragment_owned()
								));
							}
						});
					} else {
						result.push(0);
					}
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::int2_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
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
								return err!(not_can_not_applied_to_number(
									prefix.full_fragment_owned()
								));
							}
						});
					} else {
						result.push(0);
					}
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::int4_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
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
								return err!(not_can_not_applied_to_number(
									prefix.full_fragment_owned()
								));
							}
						});
					} else {
						result.push(0);
					}
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::int8_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
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
								return err!(not_can_not_applied_to_number(
									prefix.full_fragment_owned()
								));
							}
						});
					} else {
						result.push(0);
					}
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::int16_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
						data: ColumnData::int16_with_bitvec(result, container.bitvec()),
					}),
				})
			}

			ColumnData::Utf8 {
				container: _,
				..
			} => match prefix.operator {
				PrefixOperator::Not(_) => {
					err!(operator::not_can_not_applied_to_text(prefix.full_fragment_owned()))
				}
				_ => err!(frame_error("Cannot apply arithmetic prefix operator to text".to_string())),
			},

			ColumnData::Uint1(container) => {
				let mut result = Vec::with_capacity(container.data().len());
				for val in container.data().iter() {
					let signed = *val as i8;
					result.push(match prefix.operator {
						PrefixOperator::Minus(_) => -signed,
						PrefixOperator::Plus(_) => signed,
						PrefixOperator::Not(_) => {
							return err!(not_can_not_applied_to_number(
								prefix.full_fragment_owned()
							));
						}
					});
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::int1_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
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
							return err!(not_can_not_applied_to_number(
								prefix.full_fragment_owned()
							));
						}
					});
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::int2_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
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
							return err!(not_can_not_applied_to_number(
								prefix.full_fragment_owned()
							));
						}
					});
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::int4_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
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
							return err!(not_can_not_applied_to_number(
								prefix.full_fragment_owned()
							));
						}
					});
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::int8_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
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
							return err!(not_can_not_applied_to_number(
								prefix.full_fragment_owned()
							));
						}
					});
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::int16_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
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
					err!(not_can_not_applied_to_temporal(prefix.full_fragment_owned()))
				}
				_ => unimplemented!(),
			},
			ColumnData::DateTime(_) => match prefix.operator {
				PrefixOperator::Not(_) => {
					err!(not_can_not_applied_to_temporal(prefix.full_fragment_owned()))
				}
				_ => unimplemented!(),
			},
			ColumnData::Time(_) => match prefix.operator {
				PrefixOperator::Not(_) => {
					err!(not_can_not_applied_to_temporal(prefix.full_fragment_owned()))
				}
				_ => unimplemented!(),
			},
			ColumnData::Interval(_) => match prefix.operator {
				PrefixOperator::Not(_) => {
					err!(not_can_not_applied_to_temporal(prefix.full_fragment_owned()))
				}
				_ => unimplemented!(),
			},
			ColumnData::RowNumber(_) => match prefix.operator {
				PrefixOperator::Not(_) => {
					err!(not_can_not_applied_to_number(prefix.full_fragment_owned()))
				}
				_ => unimplemented!(),
			},
			ColumnData::IdentityId(_) => match prefix.operator {
				PrefixOperator::Not(_) => {
					err!(not_can_not_applied_to_uuid(prefix.full_fragment_owned()))
				}
				_ => unimplemented!(),
			},
			ColumnData::Uuid4(_) => match prefix.operator {
				PrefixOperator::Not(_) => {
					err!(not_can_not_applied_to_uuid(prefix.full_fragment_owned()))
				}
				_ => unimplemented!(),
			},
			ColumnData::Uuid7(_) => match prefix.operator {
				PrefixOperator::Not(_) => {
					err!(not_can_not_applied_to_uuid(prefix.full_fragment_owned()))
				}
				_ => unimplemented!(),
			},
			ColumnData::Blob {
				container: _,
				..
			} => match prefix.operator {
				PrefixOperator::Not(_) => {
					err!(frame_error("Cannot apply NOT operator to BLOB".to_string()))
				}
				_ => err!(frame_error("Cannot apply arithmetic prefix operator to BLOB".to_string())),
			},
			ColumnData::Int {
				container,
				..
			} => {
				let mut result = Vec::with_capacity(container.data().len());
				for (idx, val) in container.data().iter().enumerate() {
					if container.is_defined(idx) {
						result.push(match prefix.operator {
							PrefixOperator::Minus(_) => Int(-val.0.clone()),
							PrefixOperator::Plus(_) => val.clone(),
							PrefixOperator::Not(_) => {
								return err!(not_can_not_applied_to_number(
									prefix.full_fragment_owned()
								));
							}
						});
					} else {
						result.push(Int::zero());
					}
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::int_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
						data: ColumnData::int_with_bitvec(result, container.bitvec()),
					}),
				})
			}
			ColumnData::Uint {
				container,
				..
			} => match prefix.operator {
				PrefixOperator::Minus(_) => {
					let mut result = Vec::with_capacity(container.data().len());
					for (idx, val) in container.data().iter().enumerate() {
						if container.is_defined(idx) {
							let negated = -val.0.clone();
							result.push(Int::from(negated));
						} else {
							result.push(Int::zero());
						}
					}
					Ok(match column.table() {
						Some(table) => Column::SourceQualified(SourceQualified {
							source: table.clone(),
							name: column.name().clone(),
							data: ColumnData::int_with_bitvec(result, container.bitvec()),
						}),
						None => Column::ColumnQualified(ColumnQualified {
							name: column.name().clone(),
							data: ColumnData::int_with_bitvec(result, container.bitvec()),
						}),
					})
				}
				PrefixOperator::Plus(_) => {
					let mut result = Vec::with_capacity(container.data().len());
					for (idx, val) in container.data().iter().enumerate() {
						if container.is_defined(idx) {
							result.push(val.clone());
						} else {
							result.push(Uint::zero());
						}
					}
					Ok(match column.table() {
						Some(table) => Column::SourceQualified(SourceQualified {
							source: table.clone(),
							name: column.name().clone(),
							data: ColumnData::uint_with_bitvec(result, container.bitvec()),
						}),
						None => Column::ColumnQualified(ColumnQualified {
							name: column.name().clone(),
							data: ColumnData::uint_with_bitvec(result, container.bitvec()),
						}),
					})
				}
				PrefixOperator::Not(_) => {
					err!(not_can_not_applied_to_number(prefix.full_fragment_owned()))
				}
			},
			ColumnData::Decimal {
				container,
				..
			} => {
				let mut result = Vec::with_capacity(container.data().len());
				for (idx, val) in container.data().iter().enumerate() {
					if container.is_defined(idx) {
						result.push(match prefix.operator {
							PrefixOperator::Minus(_) => val.clone().negate(),
							PrefixOperator::Plus(_) => val.clone(),
							PrefixOperator::Not(_) => {
								return err!(not_can_not_applied_to_number(
									prefix.full_fragment_owned()
								));
							}
						});
					} else {
						result.push(Decimal::from(0));
					}
				}
				Ok(match column.table() {
					Some(table) => Column::SourceQualified(SourceQualified {
						source: table.clone(),
						name: column.name().clone(),
						data: ColumnData::decimal_with_bitvec(result, container.bitvec()),
					}),
					None => Column::ColumnQualified(ColumnQualified {
						name: column.name().clone(),
						data: ColumnData::decimal_with_bitvec(result, container.bitvec()),
					}),
				})
			}
		}
	}
}
