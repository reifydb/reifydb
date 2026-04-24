// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::value::{container::temporal::TemporalContainer, date::Date, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct DateTrunc {
	info: FunctionInfo,
}

impl Default for DateTrunc {
	fn default() -> Self {
		Self::new()
	}
}

impl DateTrunc {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("date::trunc"),
		}
	}
}

impl Function for DateTrunc {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Date
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 2 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		let date_col = &args[0];
		let prec_col = &args[1];
		let (date_data, date_bitvec) = date_col.unwrap_option();
		let (prec_data, prec_bitvec) = prec_col.unwrap_option();
		let row_count = date_data.len();

		let result_data = match (date_data, prec_data) {
			(
				ColumnBuffer::Date(date_container),
				ColumnBuffer::Utf8 {
					container: prec_container,
					..
				},
			) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (date_container.get(i), prec_container.is_defined(i)) {
						(Some(d), true) => {
							let precision = &prec_container[i];
							let truncated =
								match precision.as_str() {
									"year" => Date::new(d.year(), 1, 1),
									"month" => Date::new(d.year(), d.month(), 1),
									other => {
										return Err(FunctionError::ExecutionFailed {
										function: ctx.fragment.clone(),
										reason: format!("invalid precision: '{}'", other),
									});
									}
								};
							match truncated {
								Some(val) => container.push(val),
								None => container.push_default(),
							}
						}
						_ => container.push_default(),
					}
				}

				ColumnBuffer::Date(container)
			}
			(ColumnBuffer::Date(_), other) => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 1,
					expected: vec![Type::Utf8],
					actual: other.get_type(),
				});
			}
			(other, _) => {
				return Err(FunctionError::InvalidArgumentType {
					function: ctx.fragment.clone(),
					argument_index: 0,
					expected: vec![Type::Date],
					actual: other.get_type(),
				});
			}
		};

		let final_data = match (date_bitvec, prec_bitvec) {
			(Some(bv), _) | (_, Some(bv)) => ColumnBuffer::Option {
				inner: Box::new(result_data),
				bitvec: bv.clone(),
			},
			_ => result_data,
		};

		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}
