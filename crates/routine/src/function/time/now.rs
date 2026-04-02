// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::{container::temporal::TemporalContainer, datetime::DateTime, r#type::Type};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct TimeNow {
	info: FunctionInfo,
}

impl Default for TimeNow {
	fn default() -> Self {
		Self::new()
	}
}

impl TimeNow {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("time::now"),
		}
	}
}

impl Function for TimeNow {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Time
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if !args.is_empty() {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		let millis = ctx.runtime_context.clock.now_millis();
		let dt = DateTime::from_timestamp_millis(millis)?;
		let time = dt.time();

		// For zero-arg functions, we produce a single row
		let mut container = TemporalContainer::with_capacity(1);
		container.push(time);

		let result_data = ColumnData::Time(container);
		Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), result_data)]))
	}
}
