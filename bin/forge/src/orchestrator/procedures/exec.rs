// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{fs, process::Command};

use reifydb_core::value::column::columns::Columns;
use reifydb_catalog::procedure::{Procedure, context::ProcedureContext, error::ProcedureError};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};

/// Executes a shell command in a workspace directory, captures stdout/stderr to files,
/// and returns the exit code.
///
/// Expects 2 positional arguments: command (Utf8), workspace (Utf8 — directory path).
pub struct ExecProcedure;

impl Procedure for ExecProcedure {
	fn call(&self, ctx: &ProcedureContext, _tx: &mut Transaction<'_>) -> Result<Columns, ProcedureError> {
		let (command_str, workspace) = match ctx.params {
			Params::Positional(args) if args.len() >= 2 => {
				let cmd = match &args[0] {
					Value::Utf8(s) => s.clone(),
					_ => {
						return Err(ProcedureError::InvalidArgumentType {
							procedure: Fragment::internal("forge::exec"),
							argument_index: 0,
							expected: vec![Type::Utf8],
							actual: args[0].get_type(),
						});
					}
				};
				let ws = match &args[1] {
					Value::Utf8(s) => s.clone(),
					_ => {
						return Err(ProcedureError::InvalidArgumentType {
							procedure: Fragment::internal("forge::exec"),
							argument_index: 1,
							expected: vec![Type::Utf8],
							actual: args[1].get_type(),
						});
					}
				};
				(cmd, ws)
			}
			Params::Positional(args) => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("forge::exec"),
					expected: 2,
					actual: args.len(),
				});
			}
			_ => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("forge::exec"),
					expected: 2,
					actual: 0,
				});
			}
		};

		// Execute shell command with CWD set to workspace
		let output = Command::new("sh").arg("-c").arg(&command_str).current_dir(&workspace).output().map_err(
			|e| ProcedureError::ExecutionFailed {
				procedure: Fragment::internal("forge::exec"),
				reason: format!("failed to spawn: {}", e),
			},
		)?;

		let exit_code = output.status.code().unwrap_or(-1);

		// Write stdout and stderr to files in workspace
		let _ = fs::write(format!("{workspace}/stdout.log"), &output.stdout);
		let _ = fs::write(format!("{workspace}/stderr.log"), &output.stderr);

		Ok(Columns::single_row([("exit_code", Value::Int4(exit_code))]))
	}
}
