// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns, data::ColumnData};
use reifydb_type::{
	util::bitvec::BitVec,
	value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type},
};

use crate::function::{Function, FunctionCapability, FunctionContext, FunctionInfo, error::FunctionError};

pub struct TextConcat {
	info: FunctionInfo,
}

impl Default for TextConcat {
	fn default() -> Self {
		Self::new()
	}
}

impl TextConcat {
	pub fn new() -> Self {
		Self {
			info: FunctionInfo::new("text::concat"),
		}
	}
}

impl Function for TextConcat {
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
		if args.len() < 2 {
			return Err(FunctionError::ArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		// Unwrap options for each column individually
		let mut unwrapped: Vec<(&ColumnData, Option<&BitVec>)> = Vec::with_capacity(args.len());
		for col in args.iter() {
			unwrapped.push(col.data().unwrap_option());
		}

		let row_count = unwrapped[0].0.len();

		// Validate all arguments are Utf8
		for (idx, (data, _)) in unwrapped.iter().enumerate() {
			match data {
				ColumnData::Utf8 {
					..
				} => {}
				other => {
					return Err(FunctionError::InvalidArgumentType {
						function: ctx.fragment.clone(),
						argument_index: idx,
						expected: vec![Type::Utf8],
						actual: other.get_type(),
					});
				}
			}
		}

		let mut result_data = Vec::with_capacity(row_count);

		for i in 0..row_count {
			let mut all_defined = true;
			let mut concatenated = String::new();

			for (data, _) in unwrapped.iter() {
				if let ColumnData::Utf8 {
					container,
					..
				} = data
				{
					if container.is_defined(i) {
						concatenated.push_str(&container[i]);
					} else {
						all_defined = false;
						break;
					}
				}
			}

			if all_defined {
				result_data.push(concatenated);
			} else {
				result_data.push(String::new());
			}
		}

		let result_col_data = ColumnData::Utf8 {
			container: Utf8Container::new(result_data),
			max_bytes: MaxBytes::MAX,
		};

		// Combine all bitvecs
		let mut combined_bv: Option<BitVec> = None;
		for (_, bv) in unwrapped.iter() {
			if let Some(bv) = bv {
				combined_bv = Some(match combined_bv {
					Some(existing) => existing.and(bv),
					None => (*bv).clone(),
				});
			}
		}

		let final_data = match combined_bv {
			Some(bv) => ColumnData::Option {
				inner: Box::new(result_col_data),
				bitvec: bv,
			},
			None => result_col_data,
		};
		Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), final_data)]))
	}
}
