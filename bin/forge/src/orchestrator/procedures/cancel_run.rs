// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::LazyLock;

use reifydb_core::value::column::columns::Columns;
use reifydb_routine::routine::{ProcedureContext, Routine, RoutineError, RoutineInfo};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("forge::cancel_run"));

/// Cancels a pipeline run: skips all pending/blocked job_runs and pending step_runs.
///
/// Expects 1 positional argument: run_id (Uuid4).
pub struct CancelRunProcedure;

impl CancelRunProcedure {
	pub fn new() -> Self {
		Self
	}
}

impl Default for CancelRunProcedure {
	fn default() -> Self {
		Self::new()
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for CancelRunProcedure {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}
	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}
	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let run_id = match ctx.params {
			Params::Positional(args) if !args.is_empty() => args[0].clone(),
			Params::Positional(args) => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("forge::cancel_run"),
					expected: 1,
					actual: args.len(),
				});
			}
			_ => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("forge::cancel_run"),
					expected: 1,
					actual: 0,
				});
			}
		};

		let run_id_str = match &run_id {
			Value::Uuid4(u) => u.to_string(),
			Value::Utf8(s) => s.clone(),
			_ => {
				return Err(RoutineError::ProcedureInvalidArgumentType {
					procedure: Fragment::internal("forge::cancel_run"),
					argument_index: 0,
					expected: vec![Type::Uuid4, Type::Utf8],
					actual: run_id.get_type(),
				});
			}
		};

		// Update run status to cancelled
		ctx.tx.rql(
			&format!(
				"UPDATE forge::runs {{ status: \"cancelled\", finished_at: datetime::now() }} \
				 FILTER id == uuid::v4(\"{run_id_str}\")"
			),
			Params::None,
		)
		.check()?;

		// Skip all pending and blocked job_runs
		ctx.tx.rql(
			&format!(
				"UPDATE forge::job_runs {{ status: \"skipped\" }} \
				 FILTER run_id == uuid::v4(\"{run_id_str}\") AND status == \"pending\""
			),
			Params::None,
		)
		.check()?;

		ctx.tx.rql(
			&format!(
				"UPDATE forge::job_runs {{ status: \"skipped\" }} \
				 FILTER run_id == uuid::v4(\"{run_id_str}\") AND status == \"blocked\""
			),
			Params::None,
		)
		.check()?;

		// Skip all pending step_runs
		ctx.tx.rql(
			&format!(
				"UPDATE forge::step_runs {{ status: \"skipped\" }} \
				 FILTER run_id == uuid::v4(\"{run_id_str}\") AND status == \"pending\""
			),
			Params::None,
		)
		.check()?;

		Ok(Columns::single_row([
			("run_id", Value::Utf8(run_id_str)),
			("status", Value::Utf8("cancelled".to_string())),
		]))
	}
}
