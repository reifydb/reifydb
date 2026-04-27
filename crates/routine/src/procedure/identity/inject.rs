// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::LazyLock;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};

use crate::routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("identity::inject"));

/// Procedure that injects a new identity into the current session.
///
/// Takes 1 positional parameter: the IdentityId to inject.
/// Returns a single-column result containing the IdentityId value;
/// the VM intercepts this result and updates its identity accordingly.
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

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::IdentityId
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let identity_id = match ctx.params {
			Params::Positional(args) if args.len() == 1 => match &args[0] {
				Value::IdentityId(id) => *id,
				other => {
					return Err(RoutineError::ProcedureInvalidArgumentType {
						procedure: Fragment::internal("identity::inject"),
						argument_index: 0,
						expected: vec![Type::IdentityId],
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
