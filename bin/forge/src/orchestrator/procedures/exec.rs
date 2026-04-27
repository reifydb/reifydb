// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{fs, process::Command, sync::LazyLock};

use reifydb_core::value::column::columns::Columns;
use reifydb_routine::routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("forge::exec"));

/// Executes a shell command in a workspace directory, captures stdout/stderr to files,
/// and returns the exit code.
///
/// Expects 2 positional arguments: command (Utf8), workspace (Utf8 - directory path).
pub struct ExecProcedure;

impl ExecProcedure {
	pub fn new() -> Self {
		Self
	}
}

impl Default for ExecProcedure {
	fn default() -> Self {
		Self::new()
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for ExecProcedure {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}
	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}
	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let (command_str, workspace) = match ctx.params {
			Params::Positional(args) if args.len() >= 2 => {
				let cmd = match &args[0] {
					Value::Utf8(s) => s.clone(),
					_ => {
						return Err(RoutineError::ProcedureInvalidArgumentType {
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
						return Err(RoutineError::ProcedureInvalidArgumentType {
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
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("forge::exec"),
					expected: 2,
					actual: args.len(),
				});
			}
			_ => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("forge::exec"),
					expected: 2,
					actual: 0,
				});
			}
		};

		// Execute shell command with CWD set to workspace
		let output = Command::new("sh").arg("-c").arg(&command_str).current_dir(&workspace).output().map_err(
			|e| RoutineError::ProcedureExecutionFailed {
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
