// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::{blob::Blob, r#type::Type};
use reifydb_wire_format::{encode::encode_frames, json::frames_from_json, options::EncodeOptions};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

/// `rbcf::encode(Utf8) -> Blob` — parses each cell as a JSON `[ResponseFrame, ...]`
/// document and emits the RBCF binary encoding of those frames.
pub struct Encode {
	info: FunctionInfo,
}

impl Default for Encode {
	fn default() -> Self {
		Self::new()
	}
}

impl Encode {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("rbcf::encode"),
		}
	}
}

impl Function for Encode {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Blob
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if args.len() != 1 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		match column.data() {
			ColumnData::Utf8 {
				container,
				..
			} => {
				let mut result = Vec::with_capacity(container.data().len());
				for i in 0..container.data().len() {
					let json = &container[i];
					let frames =
						frames_from_json(json).map_err(|e| FunctionError::ExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!("malformed frame JSON: {}", e),
						})?;
					let bytes = encode_frames(&frames, &EncodeOptions::default()).map_err(|e| {
						FunctionError::ExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!("rbcf encode failed: {}", e),
						}
					})?;
					result.push(Blob::new(bytes));
				}
				let data = ColumnData::blob(result);
				Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), data)]))
			}
			other => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
