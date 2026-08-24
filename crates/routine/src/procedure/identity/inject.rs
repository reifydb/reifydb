// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_routine_abi::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	params::Params,
	value::{Value, value_type::ValueType},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("identity::inject"));

pub struct IdentityInject;

impl Default for IdentityInject {
	fn default() -> Self {
		Self::new()
	}
}

impl IdentityInject {
	pub fn new() -> Self {
		Self
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for IdentityInject {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[ValueType]) -> ValueType {
		ValueType::IdentityId
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		if !matches!(ctx.tx, Transaction::Test(..)) {
			return Err(RoutineError::ProcedureExecutionFailed {
				procedure: Fragment::internal("identity::inject"),
				reason: "must run in a test transaction".to_string(),
			});
		}

		let identity_id = match ctx.params {
			Params::Positional(args) if args.len() == 1 => match &args[0] {
				Value::IdentityId(id) => *id,
				other => {
					return Err(RoutineError::ProcedureInvalidArgumentType {
						procedure: Fragment::internal("identity::inject"),
						argument_index: 0,
						expected: vec![ValueType::IdentityId],
						actual: other.get_type(),
					});
				}
			},
			Params::Positional(args) => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("identity::inject"),
					expected: 1,
					actual: args.len(),
				});
			}
			_ => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("identity::inject"),
					expected: 1,
					actual: 0,
				});
			}
		};

		let col = ColumnWithName::new("identity_id", ColumnBuffer::identity_id(vec![identity_id]));
		Ok(Columns::new(vec![col]))
	}
}
