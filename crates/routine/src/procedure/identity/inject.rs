// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};

use crate::procedure::{Procedure, context::ProcedureContext, error::ProcedureError};

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

impl Procedure for IdentityInject {
	fn call(&self, ctx: &ProcedureContext, _tx: &mut Transaction<'_>) -> Result<Columns, ProcedureError> {
		let identity_id = match ctx.params {
			Params::Positional(args) if args.len() == 1 => match &args[0] {
				Value::IdentityId(id) => *id,
				other => {
					return Err(ProcedureError::InvalidArgumentType {
						procedure: Fragment::internal("identity::inject"),
						argument_index: 0,
						expected: vec![Type::IdentityId],
						actual: other.get_type(),
					});
				}
			},
			Params::Positional(args) => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("identity::inject"),
					expected: 1,
					actual: args.len(),
				});
			}
			_ => {
				return Err(ProcedureError::ArityMismatch {
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
