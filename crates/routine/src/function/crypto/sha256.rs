// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef, LargeStringArray};
use arrow_schema::FieldRef;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::value::{
	column_view::{ColumnView, ViewData},
	constraint::bytes::MaxBytes,
	value_type::ValueType,
};
use sha2::{Digest, Sha256};

use crate::function::support::column::utf8_column;

pub struct CryptoSha256 {
	info: RoutineInfo,
}

impl Default for CryptoSha256 {
	fn default() -> Self {
		Self::new()
	}
}

impl CryptoSha256 {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("crypto::sha256"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for CryptoSha256 {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(
		&self,
		ctx: &mut FunctionContext<'a>,
		args: &[(FieldRef, ArrayRef)],
	) -> Result<(FieldRef, ArrayRef), RoutineError> {
		let data = ColumnView::try_from(&args[0])?;
		let row_count = data.len();

		match &data.data {
			ViewData::Utf8 {
				container,
				..
			} => {
				let mut result_data = Vec::with_capacity(container.len());

				for i in 0..row_count {
					if i < container.len() {
						let original_str = container.value(i);
						let digest = Sha256::digest(original_str.as_bytes());
						let hex = digest.iter().map(|b| format!("{b:02x}")).collect::<String>();
						result_data.push(hex);
					} else {
						result_data.push(String::new());
					}
				}

				Ok(utf8_column(ctx.fragment.text(), MaxBytes::MAX, LargeStringArray::from(result_data)))
			}
			_ => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: data.get_type(),
			}),
		}
	}
}

impl Function for CryptoSha256 {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
