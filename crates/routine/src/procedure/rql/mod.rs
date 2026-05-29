// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod ast;
pub mod explain;
pub mod logical;
pub mod tokenize;

use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{Value, value_type::ValueType},
};

use crate::routine::error::RoutineError;

pub(super) fn extract_query(params: &Params, procedure: &'static str) -> Result<String, RoutineError> {
	match params {
		Params::Positional(args) if args.len() == 1 => match &args[0] {
			Value::Utf8(s) => Ok(s.as_str().to_string()),
			other => Err(RoutineError::ProcedureInvalidArgumentType {
				procedure: Fragment::internal(procedure),
				argument_index: 0,
				expected: vec![ValueType::Utf8],
				actual: other.get_type(),
			}),
		},
		Params::Positional(args) => Err(RoutineError::ProcedureArityMismatch {
			procedure: Fragment::internal(procedure),
			expected: 1,
			actual: args.len(),
		}),
		_ => Err(RoutineError::ProcedureArityMismatch {
			procedure: Fragment::internal(procedure),
			expected: 1,
			actual: 0,
		}),
	}
}
