// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{fs, process::Command};

use reifydb_core::{internal_error, value::column::columns::Columns};
use reifydb_engine::procedure::{Procedure, context::ProcedureContext};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, params::Params, value::Value};

/// Executes a shell command in a workspace directory, captures stdout/stderr to files,
/// and returns the exit code.
///
/// Expects 2 positional arguments: command (Utf8), workspace (Utf8 — directory path).
pub struct ExecProcedure;

impl Procedure for ExecProcedure {
	fn call(&self, ctx: &ProcedureContext, _tx: &mut Transaction<'_>) -> Result<Columns> {
		let (command_str, workspace) = match ctx.params {
			Params::Positional(args) if args.len() >= 2 => {
				let cmd = match &args[0] {
					Value::Utf8(s) => s.clone(),
					_ => return Err(internal_error!("forge::exec: command must be Utf8")),
				};
				let ws = match &args[1] {
					Value::Utf8(s) => s.clone(),
					_ => return Err(internal_error!("forge::exec: workspace must be Utf8")),
				};
				(cmd, ws)
			}
			_ => {
				return Err(internal_error!(
					"forge::exec requires 2 positional arguments: command, workspace"
				));
			}
		};

		// Execute shell command with CWD set to workspace
		let output = Command::new("sh")
			.arg("-c")
			.arg(&command_str)
			.current_dir(&workspace)
			.output()
			.map_err(|e| internal_error!("forge::exec: failed to spawn: {}", e))?;

		let exit_code = output.status.code().unwrap_or(-1);

		// Write stdout and stderr to files in workspace
		let _ = fs::write(format!("{workspace}/stdout.log"), &output.stdout);
		let _ = fs::write(format!("{workspace}/stderr.log"), &output.stderr);

		Ok(Columns::single_row([("exit_code", Value::Int4(exit_code))]))
	}
}
