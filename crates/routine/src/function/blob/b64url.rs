// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use reifydb_core::value::column::factory::blob;
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_value::{
	fragment::Fragment,
	value::{
		blob::Blob,
		column_view::{ColumnView, ViewData},
		value_type::ValueType,
	},
};

pub struct BlobB64url {
	info: RoutineInfo,
}

impl Default for BlobB64url {
	fn default() -> Self {
		Self::new()
	}
}

impl BlobB64url {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("blob::b64url"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for BlobB64url {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Blob
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
					let b64url_str = container.value(i);
					let blob = Blob::from_b64url(Fragment::internal(b64url_str))?;
					result_data.push(blob);
				}

				Ok(blob(ctx.fragment.text(), result_data))
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

impl Function for BlobB64url {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
