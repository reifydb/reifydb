// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{str::FromStr, sync::LazyLock};

use reifydb_catalog::error::CatalogError;
use reifydb_core::{interface::catalog::config::ConfigKey, value::column::columns::Columns};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	error::Error as TypeError,
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};

use crate::routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("system::config::set"));

/// Native procedure that sets a configuration value.
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

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for SetConfigProcedure {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}

	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}

	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let (key, value) = match ctx.params {
			Params::Positional(args) if args.len() == 2 => (args[0].clone(), args[1].clone()),
			Params::Positional(args) => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("system::config::set"),
					expected: 2,
					actual: args.len(),
				});
			}
			_ => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("system::config::set"),
					expected: 2,
					actual: 0,
				});
			}
		};

		let key_str = match &key {
			Value::Utf8(s) => s.as_str().to_string(),
			_ => {
				return Err(RoutineError::ProcedureInvalidArgumentType {
					procedure: Fragment::internal("system::config::set"),
					argument_index: 0,
					expected: vec![Type::Utf8],
					actual: key.get_type(),
				});
			}
		};

		if matches!(value, Value::None { .. }) {
			return Err(CatalogError::ConfigValueInvalid(key_str).into());
		}

		let config_key = match ConfigKey::from_str(&key_str) {
			Ok(k) => k,
			Err(_) => {
				return Err(CatalogError::ConfigStorageKeyNotFound(key_str).into());
			}
		};

		let coerced_value = config_key.accept(value).map_err(|e| {
			RoutineError::Wrapped(Box::new(TypeError::from(CatalogError::from((config_key, e)))))
		})?;

		let value_clone = coerced_value.clone();

		match ctx.tx {
			Transaction::Admin(admin) => ctx.catalog.set_config(admin, config_key, coerced_value)?,
			Transaction::Test(t) => ctx.catalog.set_config(t.inner, config_key, coerced_value)?,
			_ => {
				return Err(RoutineError::ProcedureExecutionFailed {
					procedure: Fragment::internal("system::config::set"),
					reason: "must run in an admin transaction".to_string(),
				});
			}
		}

		Ok(Columns::single_row([("key", Value::Utf8(key_str)), ("value", value_clone)]))
	}
}
