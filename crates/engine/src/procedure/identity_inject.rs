// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	internal_error,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::Value;

use super::{Procedure, context::ProcedureContext};

/// Procedure that injects a new identity into the current session.
///
/// Takes 1 positional parameter: the IdentityId to inject.
/// Returns a single-column result containing the IdentityId value;
/// the VM intercepts this result and updates its identity accordingly.
pub struct IdentityInject;

impl IdentityInject {
	pub fn new() -> Self {
		Self
	}
}

impl Procedure for IdentityInject {
	fn call(&self, ctx: &ProcedureContext, _tx: &mut Transaction<'_>) -> reifydb_type::Result<Columns> {
		let identity_id = match ctx.params {
			reifydb_type::params::Params::Positional(args) if args.len() == 1 => match &args[0] {
				Value::IdentityId(id) => *id,
				other => {
					return Err(internal_error!(
						"identity::inject expects an IdentityId argument, got {:?}",
						other
					));
				}
			},
			_ => {
				return Err(internal_error!(
					"identity::inject requires exactly 1 positional IdentityId argument"
				));
			}
		};

		let col = Column::new("identity_id", ColumnData::identity_id(vec![identity_id]));
		Ok(Columns::new(vec![col]))
	}
}
