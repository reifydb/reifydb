// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_routine::procedure::{Procedure, context::ProcedureContext, error::ProcedureError};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	Result as TypeResult,
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};

/// Completes a job_run and handles cascading effects:
/// - On success: unblocks dependent jobs whose deps are all satisfied
/// - On failure: skips remaining step_runs and transitively skips dependent jobs
/// - In both cases: checks if all job_runs are terminal to finalize the run
///
/// Expects 2 positional arguments: job_run_id (Uuid4), status ("succeeded" or "failed").
pub struct CompleteJobRunProcedure;

impl Procedure for CompleteJobRunProcedure {
	fn call(&self, ctx: &ProcedureContext, tx: &mut Transaction<'_>) -> Result<Columns, ProcedureError> {
		let (job_run_id_str, status_str) = match ctx.params {
			Params::Positional(args) if args.len() >= 2 => {
				let id = match &args[0] {
					Value::Uuid4(u) => u.to_string(),
					Value::Utf8(s) => s.clone(),
					_ => {
						return Err(ProcedureError::InvalidArgumentType {
							procedure: Fragment::internal("forge::complete_job_run"),
							argument_index: 0,
							expected: vec![Type::Uuid4, Type::Utf8],
							actual: args[0].get_type(),
						});
					}
				};
				let status = match &args[1] {
					Value::Utf8(s) => s.clone(),
					_ => {
						return Err(ProcedureError::InvalidArgumentType {
							procedure: Fragment::internal("forge::complete_job_run"),
							argument_index: 1,
							expected: vec![Type::Utf8],
							actual: args[1].get_type(),
						});
					}
				};
				(id, status)
			}
			Params::Positional(args) => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("forge::complete_job_run"),
					expected: 2,
					actual: args.len(),
				});
			}
			_ => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("forge::complete_job_run"),
					expected: 2,
					actual: 0,
				});
			}
		};

		if status_str != "succeeded" && status_str != "failed" {
			return Err(ProcedureError::ExecutionFailed {
				procedure: Fragment::internal("forge::complete_job_run"),
				reason: format!("status must be \"succeeded\" or \"failed\", got \"{}\"", status_str),
			});
		}

		// Get the job_run to find run_id and job_id
		let job_run_result = tx.rql(
			&format!("FROM forge::job_runs | FILTER id == uuid::v4(\"{job_run_id_str}\")"),
			Params::None,
		)?;

		let job_run_row = job_run_result.first().and_then(|f| f.rows().next()).ok_or_else(|| {
			ProcedureError::ExecutionFailed {
				procedure: Fragment::internal("forge::complete_job_run"),
				reason: format!("Job run not found: {}", job_run_id_str),
			}
		})?;

		let run_id = job_run_row.get_value("run_id").map(|v| v.to_string()).ok_or_else(|| {
			ProcedureError::ExecutionFailed {
				procedure: Fragment::internal("forge::complete_job_run"),
				reason: format!("job run {} is missing required field 'run_id'", job_run_id_str),
			}
		})?;
		let job_id = job_run_row.get_value("job_id").map(|v| v.to_string()).ok_or_else(|| {
			ProcedureError::ExecutionFailed {
				procedure: Fragment::internal("forge::complete_job_run"),
				reason: format!("job run {} is missing required field 'job_id'", job_run_id_str),
			}
		})?;

		// Update the job_run status
		tx.rql(
			&format!(
				"UPDATE forge::job_runs {{ status: \"{status_str}\", finished_at: datetime::now() }} \
				 FILTER id == uuid::v4(\"{job_run_id_str}\")"
			),
			Params::None,
		)?;

		if status_str == "succeeded" {
			// Check blocked job_runs in this run and unblock those whose deps are all satisfied
			unblock_ready_jobs(tx, &run_id)?;
		} else {
			// Skip remaining pending step_runs in this job_run
			tx.rql(
				&format!("UPDATE forge::step_runs {{ status: \"skipped\" }} \
					 FILTER job_run_id == uuid::v4(\"{job_run_id_str}\") AND status == \"pending\""),
				Params::None,
			)?;

			// Transitively skip all dependent job_runs
			skip_dependents(tx, &run_id, &job_id)?;
		}

		// Check if all job_runs in this run are terminal → finalize the run
		let non_terminal = tx.rql(
			&format!(
				"FROM forge::job_runs | FILTER run_id == uuid::v4(\"{run_id}\") AND status != \"succeeded\" AND status != \"failed\" AND status != \"skipped\""
			),
			Params::None,
		)?;

		let all_terminal = non_terminal.first().map_or(true, |f| f.rows().next().is_none());

		if all_terminal {
			// Check if any job_run failed
			let any_failed = tx.rql(
				&format!(
					"FROM forge::job_runs | FILTER run_id == uuid::v4(\"{run_id}\") AND status == \"failed\""
				),
				Params::None,
			)?;

			let run_status = if any_failed.first().is_some_and(|f| f.rows().next().is_some()) {
				"failed"
			} else {
				"succeeded"
			};

			tx.rql(
				&format!(
					"UPDATE forge::runs {{ status: \"{run_status}\", finished_at: datetime::now() }} \
					 FILTER id == uuid::v4(\"{run_id}\")"
				),
				Params::None,
			)?;
		}

		Ok(Columns::single_row([
			("job_run_id", Value::Utf8(job_run_id_str)),
			("status", Value::Utf8(status_str)),
		]))
	}
}

fn missing_field(table: &str, field: &str) -> reifydb_type::error::Error {
	ProcedureError::ExecutionFailed {
		procedure: Fragment::internal("forge::complete_job_run"),
		reason: format!("{} row is missing required field '{}'", table, field),
	}
	.into()
}

/// For each blocked job_run in this run, check if all its dependencies have succeeded.
/// If so, transition it to "pending".
fn unblock_ready_jobs(tx: &mut Transaction<'_>, run_id: &str) -> TypeResult<()> {
	let blocked = tx.rql(
		&format!("FROM forge::job_runs | FILTER run_id == uuid::v4(\"{run_id}\") AND status == \"blocked\""),
		Params::None,
	)?;

	if let Some(frame) = blocked.first() {
		for row in frame.rows() {
			let blocked_job_run_id = row
				.get_value("id")
				.map(|v| v.to_string())
				.ok_or_else(|| missing_field("job_runs", "id"))?;
			let blocked_job_id = row
				.get_value("job_id")
				.map(|v| v.to_string())
				.ok_or_else(|| missing_field("job_runs", "job_id"))?;

			// Get all dependencies for this job
			let deps = tx.rql(
				&format!(
					"FROM forge::job_dependencies | FILTER job_id == uuid::v4(\"{blocked_job_id}\")"
				),
				Params::None,
			)?;

			let mut all_deps_satisfied = true;

			if let Some(dep_frame) = deps.first() {
				for dep_row in dep_frame.rows() {
					let dep_job_id = dep_row
						.get_value("depends_on_job_id")
						.map(|v| v.to_string())
						.ok_or_else(|| {
							missing_field("job_dependencies", "depends_on_job_id")
						})?;

					// Check if there's a succeeded job_run for this dependency in this run
					let dep_job_run = tx.rql(
						&format!(
							"FROM forge::job_runs | FILTER run_id == uuid::v4(\"{run_id}\") AND job_id == uuid::v4(\"{dep_job_id}\") AND status == \"succeeded\""
						),
						Params::None,
					)?;

					if dep_job_run.first().map_or(true, |f| f.rows().next().is_none()) {
						all_deps_satisfied = false;
						break;
					}
				}
			}

			if all_deps_satisfied {
				tx.rql(
					&format!("UPDATE forge::job_runs {{ status: \"pending\" }} \
						 FILTER id == uuid::v4(\"{blocked_job_run_id}\")"),
					Params::None,
				)?;
			}
		}
	}

	Ok(())
}

/// Transitively skip all job_runs that depend on the failed job.
fn skip_dependents(tx: &mut Transaction<'_>, run_id: &str, failed_job_id: &str) -> TypeResult<()> {
	let mut jobs_to_skip = vec![failed_job_id.to_string()];
	let mut i = 0;

	while i < jobs_to_skip.len() {
		let current_job_id = jobs_to_skip[i].clone();
		i += 1;

		// Find jobs that depend on this one
		let dependents = tx.rql(
			&format!(
				"FROM forge::job_dependencies | FILTER depends_on_job_id == uuid::v4(\"{current_job_id}\")"
			),
			Params::None,
		)?;

		if let Some(frame) = dependents.first() {
			for row in frame.rows() {
				let dependent_job_id = row
					.get_value("job_id")
					.map(|v| v.to_string())
					.ok_or_else(|| missing_field("job_dependencies", "job_id"))?;

				if !jobs_to_skip.contains(&dependent_job_id) {
					jobs_to_skip.push(dependent_job_id.clone());
				}

				// Skip the job_run for this dependent job in this run
				tx.rql(
					&format!(
						"UPDATE forge::job_runs {{ status: \"skipped\", finished_at: datetime::now() }} \
						 FILTER run_id == uuid::v4(\"{run_id}\") AND job_id == uuid::v4(\"{dependent_job_id}\") AND status != \"succeeded\" AND status != \"failed\""
					),
					Params::None,
				)?;

				// Skip step_runs for the skipped job_run
				let skipped_job_runs = tx.rql(
					&format!(
						"FROM forge::job_runs | FILTER run_id == uuid::v4(\"{run_id}\") AND job_id == uuid::v4(\"{dependent_job_id}\") AND status == \"skipped\""
					),
					Params::None,
				)?;

				if let Some(jr_frame) = skipped_job_runs.first() {
					for jr_row in jr_frame.rows() {
						let jr_id = jr_row
							.get_value("id")
							.map(|v| v.to_string())
							.ok_or_else(|| missing_field("job_runs", "id"))?;
						tx.rql(
							&format!("UPDATE forge::step_runs {{ status: \"skipped\" }} \
								 FILTER job_run_id == uuid::v4(\"{jr_id}\") AND status == \"pending\""),
							Params::None,
						)?;
					}
				}
			}
		}
	}

	Ok(())
}
