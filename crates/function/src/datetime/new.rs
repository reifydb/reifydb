// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::data::ColumnData;
use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, r#type::Type};

use crate::{ScalarFunction, ScalarFunctionContext, error::ScalarFunctionError, propagate_options};

pub struct DateTimeNew;

impl DateTimeNew {
	pub fn new() -> Self {
		Self
	}
}

impl ScalarFunction for DateTimeNew {
	fn scalar(&self, ctx: ScalarFunctionContext) -> crate::error::ScalarFunctionResult<ColumnData> {
		if let Some(result) = propagate_options(self, &ctx) {
			return result;
		}
		let columns = ctx.columns;
		let row_count = ctx.row_count;

		if columns.len() != 2 {
			return Err(ScalarFunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: columns.len(),
			});
		}

		let date_col = columns.get(0).unwrap();
		let time_col = columns.get(1).unwrap();

		match (date_col.data(), time_col.data()) {
			(ColumnData::Date(date_container), ColumnData::Time(time_container)) => {
				let mut container = TemporalContainer::with_capacity(row_count);

				for i in 0..row_count {
					match (date_container.get(i), time_container.get(i)) {
						(Some(date), Some(time)) => {
							match DateTime::new(
								date.year(),
								date.month(),
								date.day(),
								time.hour(),
								time.minute(),
								time.second(),
								time.nanosecond(),
							) {
								Some(dt) => container.push(dt),
								None => container.push_default(),
							}
						}
						_ => container.push_default(),
					}
				}

				Ok(ColumnData::DateTime(container))
			}
			(ColumnData::Date(_), other) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 1,
				expected: vec![Type::Time],
				actual: other.get_type(),
			}),
			(other, _) => Err(ScalarFunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Date],
				actual: other.get_type(),
			}),
		}
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::DateTime
	}
}
