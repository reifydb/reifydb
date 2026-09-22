// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use bumpalo::Bump;
use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{
	Arity, Function, FunctionKind, Routine, RoutineInfo, context::FunctionContext, error::RoutineError,
};
use reifydb_rql::{
	ast::parse_str,
	fingerprint::{request::fingerprint_request, statement::fingerprint_statement},
};
use reifydb_value::value::value_type::ValueType;

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

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::Utf8
	}

	fn execute(&self, ctx: &mut FunctionContext<'a>, args: &Columns) -> Result<Columns, RoutineError> {
		let data = &args[0];
		let row_count = data.len();

		match data {
			ColumnBuffer::Utf8 {
				container,
				..
			} => {
				let mut result_data = Vec::with_capacity(row_count);

				for i in 0..row_count {
					let query = container.value(i);
					let bump = Bump::new();
					let stmts = parse_str(&bump, query).map_err(|e| {
						RoutineError::FunctionExecutionFailed {
							function: ctx.fragment.clone(),
							reason: format!("{e}"),
						}
					})?;
					let fps: Vec<_> = stmts.iter().map(|s| fingerprint_statement(s)).collect();
					let req = fingerprint_request(&fps);
					result_data.push(req.to_hex());
				}

				let inner_data = ColumnBuffer::utf8(result_data);

				Ok(Columns::new(vec![ColumnWithName::new(ctx.fragment.clone(), inner_data)]))
			}
			other => Err(RoutineError::FunctionInvalidArgumentType {
				function: ctx.fragment.clone(),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: other.get_type(),
			}),
		}
	}
}

impl Function for RqlFingerprint {
	fn kinds(&self) -> &[FunctionKind] {
		&[FunctionKind::Scalar]
	}

	fn arity(&self) -> Arity {
		Arity::Exact(1)
	}
}
