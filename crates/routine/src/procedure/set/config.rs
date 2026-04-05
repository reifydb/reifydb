// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::str::FromStr;

use reifydb_core::{interface::catalog::config::SystemConfigKey, value::column::columns::Columns};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};

use crate::procedure::{Procedure, context::ProcedureContext, error::ProcedureError};

/// Native procedure that sets a system configuration value.
///
/// Accepts 2 positional arguments: key (Utf8) and value (any).
pub struct SetConfigProcedure;

impl Default for SetConfigProcedure {
	fn default() -> Self {
		Self::new()
	}
}

impl SetConfigProcedure {
	pub fn new() -> Self {
		Self
	}
}

impl Procedure for SetConfigProcedure {
	fn call(&self, ctx: &ProcedureContext, tx: &mut Transaction<'_>) -> Result<Columns, ProcedureError> {
		let (key, value) = match ctx.params {
			Params::Positional(args) if args.len() == 2 => (args[0].clone(), args[1].clone()),
			Params::Positional(args) => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("system::config::set"),
					expected: 2,
					actual: args.len(),
				});
			}
			_ => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("system::config::set"),
					expected: 2,
					actual: 0,
				});
			}
		};

		let key_str = match &key {
			Value::Utf8(s) => s.as_str().to_string(),
			_ => {
				return Err(ProcedureError::InvalidArgumentType {
					procedure: Fragment::internal("system::config::set"),
					argument_index: 0,
					expected: vec![Type::Utf8],
					actual: key.get_type(),
				});
			}
		};

		let value_clone = value.clone();

		let system_config_key = match SystemConfigKey::from_str(&key_str) {
			Ok(k) => k,
			Err(e) => {
				return Err(ProcedureError::ExecutionFailed {
					procedure: Fragment::internal("system::config::set"),
					reason: e,
				});
			}
		};

		match tx {
			Transaction::Admin(admin) => ctx.catalog.set_system_config(admin, system_config_key, value)?,
			Transaction::Test(t) => ctx.catalog.set_system_config(t.inner, system_config_key, value)?,
			_ => {
				return Err(ProcedureError::ExecutionFailed {
					procedure: Fragment::internal("system::config::set"),
					reason: "must run in an admin transaction".to_string(),
				});
			}
		}

		Ok(Columns::single_row([("key", Value::Utf8(key_str)), ("value", value_clone)]))
	}
}
