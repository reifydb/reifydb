// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	util::bitvec::BitVec,
	value::{constraint::bytes::MaxBytes, container::utf8::Utf8Container, r#type::Type},
};

use crate::routine::{Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError};

pub struct TextConcat {
	info: RoutineInfo,
}

impl Default for TextConcat {
	fn default() -> Self {
		Self::new()
	}
}

impl TextConcat {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("text::concat"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for TextConcat {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() < 2 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.fragment.clone(),
				expected: 2,
				actual: args.len(),
			});
		}

		// Unwrap options for each column individually
		let mut unwrapped: Vec<(&ColumnBuffer, Option<&BitVec>)> = Vec::with_capacity(args.len());
		for col in args.iter() {
			unwrapped.push(col.data().unwrap_option());
		}

		let row_count = unwrapped[0].0.len();

		// Validate all arguments are Utf8
		for (idx, (data, _)) in unwrapped.iter().enumerate() {
			match data {
				ColumnBuffer::Utf8 {
					..
				} => {}
				other => {
					return Err(RoutineError::FunctionInvalidArgumentType {
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
				if let ColumnBuffer::Utf8 {
					container,
					..
				} = data
				{
					if container.is_defined(i) {
						concatenated.push_str(container.get(i).unwrap());
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

		let result_col_data = ColumnBuffer::Utf8 {
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
			Some(bv) => ColumnBuffer::Option {
				inner: Box::new(result_col_data),
				bitvec: bv,
			},
			None => result_col_data,
		};
		Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), final_data)]))
	}
}

impl Function for TextConcat {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}
}
