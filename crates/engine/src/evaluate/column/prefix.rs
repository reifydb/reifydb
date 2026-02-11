// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::diagnostic::engine::frame_error,
	value::column::{Column, data::ColumnData},
};
use reifydb_function::registry::Functions;
use reifydb_rql::expression::{PrefixExpression, PrefixOperator};
use reifydb_runtime::clock::Clock;
use reifydb_type::{
	err,
	error::diagnostic::{
		operator,
		operator::{
			not_can_not_applied_to_number, not_can_not_applied_to_temporal, not_can_not_applied_to_uuid,
		},
	},
	value::{decimal::Decimal, int::Int, uint::Uint},
};

use crate::evaluate::EvalContext;

pub(crate) fn prefix_eval(
	ctx: &EvalContext,
	prefix: &PrefixExpression,
	functions: &Functions,
	clock: &Clock,
) -> crate::Result<Column> {
	let inner_ctx = EvalContext {
		target: None,
		columns: ctx.columns.clone(),
		row_count: ctx.row_count,
		take: ctx.take,
		params: ctx.params,
		symbol_table: ctx.symbol_table,
		is_aggregate_context: ctx.is_aggregate_context,
		functions: ctx.functions,
		clock: ctx.clock,
	};
	let column = super::evaluate(&inner_ctx, &prefix.expression, functions, clock)?;

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

				let new_data = ColumnData::bool_with_bitvec(result, container.bitvec());
				Ok(column.with_new_data(new_data))
			}
			_ => err!(frame_error("Cannot apply arithmetic prefix operator to bool".to_string())),
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
			let new_data = ColumnData::float4_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
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
			let new_data = ColumnData::float8_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
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
			let new_data = ColumnData::int1_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
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
			let new_data = ColumnData::int2_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
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
			let new_data = ColumnData::int4_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
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
			let new_data = ColumnData::int8_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
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
			let new_data = ColumnData::int16_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
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
			let new_data = ColumnData::int1_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
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
			let new_data = ColumnData::int2_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
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
			let new_data = ColumnData::int4_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
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
			let new_data = ColumnData::int8_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
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
			let new_data = ColumnData::int16_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
		}
		// EngineColumnData::Undefined(_) => {
		//     Err("Cannot apply prefix operator to undefined data".into())
		// }
		ColumnData::Undefined(_) => Ok(column),

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
		ColumnData::Duration(_) => match prefix.operator {
			PrefixOperator::Not(_) => {
				err!(not_can_not_applied_to_temporal(prefix.full_fragment_owned()))
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
			let new_data = ColumnData::int_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
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
				let new_data = ColumnData::int_with_bitvec(result, container.bitvec());
				Ok(column.with_new_data(new_data))
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
				let new_data = ColumnData::uint_with_bitvec(result, container.bitvec());
				Ok(column.with_new_data(new_data))
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
			let new_data = ColumnData::decimal_with_bitvec(result, container.bitvec());
			Ok(column.with_new_data(new_data))
		}
		ColumnData::DictionaryId(_) => match prefix.operator {
			PrefixOperator::Not(_) => {
				err!(frame_error("Cannot apply NOT operator to DictionaryId type".to_string()))
			}
			_ => err!(frame_error(
				"Cannot apply arithmetic prefix operator to DictionaryId type".to_string()
			)),
		},
		ColumnData::Any(_) => match prefix.operator {
			PrefixOperator::Not(_) => {
				err!(frame_error("Cannot apply NOT operator to Any type".to_string()))
			}
			_ => err!(frame_error("Cannot apply arithmetic prefix operator to Any type".to_string())),
		},
	}
}
