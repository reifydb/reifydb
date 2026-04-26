// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use bumpalo::Bump;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_rql::{
	ast::parse_str,
	fingerprint::{request::fingerprint_request, statement::fingerprint_statement},
};
use reifydb_type::value::r#type::Type;

use crate::routine::{FunctionContext, FunctionKind, Routine, RoutineError, RoutineInfo};

pub struct RqlFingerprint {
	info: RoutineInfo,
}

impl Default for RqlFingerprint {
	fn default() -> Self {
		Self::new()
	}
}

impl RqlFingerprint {
	pub fn new() -> Self {
		Self {
			info: RoutineInfo::new("rql::fingerprint"),
		}
	}
}

impl<'a> Routine<FunctionContext<'a>> for RqlFingerprint {
	fn info(&self) -> &RoutineInfo {
		&self.info
	}

	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		if args.len() != 1 {
			return Err(RoutineError::FunctionArityMismatch {
				function: ctx.env.fragment.clone(),
				expected: 1,
				actual: args.len(),
			});
		}

		let column = &args[0];
		let (data, bitvec) = column.unwrap_option();
		let row_count = data.len();

		match data {
			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				let mut result_data = Vec::with_capacity(row_count);
				let mut result_bitvec = Vec::with_capacity(row_count);

				for i in 0..row_count {
					if container.is_defined(i) {
						let query = &container[i];
						let bump = Bump::new();
						let stmts = parse_str(&bump, query).map_err(|e| {
							RoutineError::FunctionExecutionFailed {
								function: ctx.env.fragment.clone(),
								reason: format!("{e}"),
							}
						})?;
						let fps: Vec<_> =
							stmts.iter().map(|s| fingerprint_statement(s)).collect();
						let req = fingerprint_request(&fps);
						result_data.push(req.to_hex());
						result_bitvec.push(true);
					} else {
						result_data.push(String::new());
						result_bitvec.push(false);
					}
				}

				let inner_data = ColumnBuffer::utf8_with_bitvec(result_data, result_bitvec);
				let final_data = match bitvec {
					Some(bv) => ColumnBuffer::Option {
						inner: Box::new(inner_data),
						bitvec: bv.clone(),
					},
					None => inner_data,
				};

				Ok(Columns::new(vec![ColumnWithName::new(ctx.env.fragment.clone(), final_data)]))
			}
			other => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.env.fragment.clone(),
				argument_index: 0,
				expected: vec![Type::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}
