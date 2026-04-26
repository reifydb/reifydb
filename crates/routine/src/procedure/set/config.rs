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
	value::{Value, duration::Duration, r#type::Type},
};

use crate::routine::{ProcedureContext, Routine, RoutineError, RoutineInfo};

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

		let coerced_value = coerce_config_value(config_key, value)
			.map_err(|e| RoutineError::Wrapped(Box::new(TypeError::from(*e))))?;

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

fn coerce_config_value(key: ConfigKey, value: Value) -> Result<Value, Box<CatalogError>> {
	let expected_types = key.expected_types();
	if expected_types.contains(&value.get_type()) {
		return Ok(value);
	}

	// Try basic coercion
	for expected in expected_types {
		match expected {
			Type::Uint8 => {
				if let Some(v) = value.to_usize()
					&& v <= u64::MAX as usize
				{
					return Ok(Value::Uint8(v as u64));
				}
			}
			Type::Uint4 => {
				if let Some(v) = value.to_usize()
					&& v <= u32::MAX as usize
				{
					return Ok(Value::Uint4(v as u32));
				}
			}
			Type::Uint2 => {
				if let Some(v) = value.to_usize()
					&& v <= u16::MAX as usize
				{
					return Ok(Value::Uint2(v as u16));
				}
			}
			Type::Uint1 => {
				if let Some(v) = value.to_usize()
					&& v <= u8::MAX as usize
				{
					return Ok(Value::Uint1(v as u8));
				}
			}
			Type::Int8 => {
				if let Some(v) = value.to_usize()
					&& v <= i64::MAX as usize
				{
					return Ok(Value::Int8(v as i64));
				}
			}
			Type::Int4 => {
				if let Some(v) = value.to_usize()
					&& v <= i32::MAX as usize
				{
					return Ok(Value::Int4(v as i32));
				}
			}
			Type::Int2 => {
				if let Some(v) = value.to_usize()
					&& v <= i16::MAX as usize
				{
					return Ok(Value::Int2(v as i16));
				}
			}
			Type::Int1 => {
				if let Some(v) = value.to_usize()
					&& v <= i8::MAX as usize
				{
					return Ok(Value::Int1(v as i8));
				}
			}
			Type::Duration => {
				if let Value::Duration(v) = value {
					return Ok(Value::Duration(v));
				}
				if let Some(v) = value.to_usize()
					&& let Ok(d) = Duration::from_seconds(v as i64)
				{
					return Ok(Value::Duration(d));
				}
			}
			_ => {}
		}
	}

	Err(Box::new(CatalogError::ConfigTypeMismatch {
		key: key.to_string(),
		expected: expected_types.to_vec(),
		actual: value.get_type(),
	}))
}
