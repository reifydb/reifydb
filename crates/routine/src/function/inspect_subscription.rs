// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::r#type::Type;

use crate::function::{
	Function, FunctionCapability, FunctionContext, FunctionInfo,
	error::{ScalarFunctionResult, FunctionError},
};

pub struct InspectSubscription {
	info: FunctionInfo,
}

impl Default for InspectSubscription {
	fn default() -> Self {
		Self::new()
	}
}

impl InspectSubscription {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("inspect_subscription"),
		}
	}
}

impl Function for InspectSubscription {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Generator]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any // This generator returns a complex structure
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> ScalarFunctionResult<Columns> {
		// This generator function is expected to be called with no arguments.
		if !args.is_empty() {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 0,
				actual: args.len(),
			});
		}

		// In a real scenario, this would query the subscription manager or context
		// and return information about active subscriptions.
		// For this example, we return a dummy column.
		let dummy_data = ColumnData::text_with_capacity(1);
		let dummy_column = Column::text("subscription_info", dummy_data);

		Ok(Columns::new(vec![dummy_column]))
	}
}
