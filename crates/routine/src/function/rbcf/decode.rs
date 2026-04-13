// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::value::r#type::Type;
use reifydb_wire_format::{decode::decode_frames, json::to::frames_to_json};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

/// `rbcf::decode(Blob) -> Utf8` — decodes each RBCF byte payload back into the
/// canonical `[ResponseFrame, ...]` JSON shape.
pub struct Decode {
	info: FunctionInfo,
}

impl Default for Decode {
	fn default() -> Self {
		Self::new()
	}
}

impl Decode {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("rbcf::decode"),
		}
	}
}

impl Function for Decode {
	fn info(&self) -> &FunctionInfo {
		&self.info
	}

	fn capabilities(&self) -> &[FunctionCapability] {
		&[FunctionCapability::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
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
			ColumnData::Blob {
				container,
				..
			} => {
				let mut result = Vec::with_capacity(container.data().len());
				for i in 0..container.data().len() {
					let bytes = container[i].as_bytes();
					let frames =
						decode_frames(bytes).map_err(|e| FunctionError::ExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!("rbcf decode failed: {}", e),
						})?;
					let json = frames_to_json(&frames).map_err(|e| {
						FunctionError::ExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!("frame json serialization failed: {}", e),
						}
					})?;
					result.push(json);
				}
				let data = ColumnData::utf8(result);
				Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), data)]))
			}
			other => Err(FunctionError::InvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Blob],
				actual: other.get_type(),
			}),
		}
	}
}
