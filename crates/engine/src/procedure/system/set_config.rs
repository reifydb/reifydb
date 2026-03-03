// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{internal_error, value::column::columns::Columns};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, params::Params, value::Value};

use super::super::{Procedure, context::ProcedureContext};

/// Native procedure that sets a system configuration value.
///
/// Accepts 2 positional arguments: key (Utf8) and value (any).
pub struct SetConfigProcedure;

impl SetConfigProcedure {
	pub fn new() -> Self {
		Self
	}
}

impl Procedure for SetConfigProcedure {
	fn call(&self, ctx: &ProcedureContext, tx: &mut Transaction<'_>) -> Result<Columns> {
		let (key, value) = match ctx.params {
			Params::Positional(args) if args.len() == 2 => (args[0].clone(), args[1].clone()),
			_ => {
				return Err(internal_error!(
					"system::config::set requires exactly 2 positional arguments"
				));
			}
		};

		let key_str = match &key {
			Value::Utf8(s) => s.as_str().to_string(),
			_ => {
				return Err(internal_error!("system::config::set: first argument (key) must be Utf8"));
			}
		};

		let value_clone = value.clone();

		match tx {
			Transaction::Admin(admin) => ctx.catalog.set_config(admin, &key_str, value)?,
			_ => {
				return Err(internal_error!("system::config::set must run in a write transaction"));
			}
		}

		Ok(Columns::single_row([("key", Value::Utf8(key_str)), ("value", value_clone)]))
	}
}
