// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_routine::procedure::{Procedure, context::ProcedureContext, error::ProcedureError};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};

/// Creates a new pipeline run with job_runs and step_runs for every job/step in the pipeline.
///
/// Expects 1 positional argument: pipeline_id (Uuid4).
///
/// Jobs with no dependencies get job_runs with status "pending" (immediately claimable).
/// Jobs with dependencies get job_runs with status "blocked" (waiting on deps).
pub struct RunPipelineProcedure;

impl Procedure for RunPipelineProcedure {
	fn call(&self, ctx: &ProcedureContext, tx: &mut Transaction<'_>) -> Result<Columns, ProcedureError> {
		let pipeline_id = match ctx.params {
			Params::Positional(args) if !args.is_empty() => args[0].clone(),
			Params::Positional(args) => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("forge::run_pipeline"),
					expected: 1,
					actual: args.len(),
				});
			}
			_ => {
				return Err(ProcedureError::ArityMismatch {
					procedure: Fragment::internal("forge::run_pipeline"),
					expected: 1,
					actual: 0,
				});
			}
		};

		let pipeline_id_str = match &pipeline_id {
			Value::Uuid4(u) => u.to_string(),
			Value::Utf8(s) => s.clone(),
			_ => {
				return Err(ProcedureError::InvalidArgumentType {
					procedure: Fragment::internal("forge::run_pipeline"),
					argument_index: 0,
					expected: vec![Type::Uuid4, Type::Utf8],
					actual: pipeline_id.get_type(),
				});
			}
		};

		// Validate pipeline exists
		let pipelines = tx.rql(
			&format!("FROM forge::pipelines | FILTER id == uuid::v4(\"{pipeline_id_str}\")"),
			Params::None,
		)?;
		if pipelines.is_empty() || pipelines[0].rows().next().is_none() {
			return Err(ProcedureError::ExecutionFailed {
				procedure: Fragment::internal("forge::run_pipeline"),
				reason: format!("Pipeline not found: {}", pipeline_id_str),
			});
		}

		// Create the run
		let run_id = uuid::Uuid::new_v4();
		tx.rql(
			&format!(
				"INSERT forge::runs [{{ id: uuid::v4(\"{run_id}\"), pipeline_id: uuid::v4(\"{pipeline_id_str}\"), \
				 status: \"pending\", triggered_by: \"manual\", started_at: datetime::now() }}]"
			),
			Params::None,
		)?;

		// Query all jobs for this pipeline
		let jobs = tx.rql(
			&format!(
				"FROM forge::jobs | FILTER pipeline_id == uuid::v4(\"{pipeline_id_str}\") | SORT {{position:ASC}}"
			),
			Params::None,
		)?;

		if let Some(job_frame) = jobs.first() {
			for job_row in job_frame.rows() {
				let job_id = job_row.get_value("id").map(|v| v.to_string()).ok_or_else(|| {
					ProcedureError::ExecutionFailed {
						procedure: Fragment::internal("forge::run_pipeline"),
						reason: "jobs row is missing required field 'id'".to_string(),
					}
				})?;

				// Check if this job has any dependencies
				let deps = tx.rql(
					&format!(
						"FROM forge::job_dependencies | FILTER job_id == uuid::v4(\"{job_id}\")"
					),
					Params::None,
				)?;

				let has_deps = deps.first().is_some_and(|f| f.rows().next().is_some());
				let job_run_status = if has_deps {
					"blocked"
				} else {
					"pending"
				};

				// Create job_run
				let job_run_id = uuid::Uuid::new_v4();
				tx.rql(
					&format!("INSERT forge::job_runs [{{ id: uuid::v4(\"{job_run_id}\"), \
						 run_id: uuid::v4(\"{run_id}\"), job_id: uuid::v4(\"{job_id}\"), \
						 status: \"{job_run_status}\" }}]"),
					Params::None,
				)?;

				// Query steps for this job and create step_runs
				let steps = tx.rql(
					&format!(
						"FROM forge::steps | FILTER job_id == uuid::v4(\"{job_id}\") | SORT {{position:ASC}}"
					),
					Params::None,
				)?;

				if let Some(step_frame) = steps.first() {
					for step_row in step_frame.rows() {
						let step_id = step_row
							.get_value("id")
							.map(|v| v.to_string())
							.ok_or_else(|| ProcedureError::ExecutionFailed {
								procedure: Fragment::internal("forge::run_pipeline"),
								reason: "steps row is missing required field 'id'"
									.to_string(),
							})?;
						let step_run_id = uuid::Uuid::new_v4();

						tx.rql(
							&format!(
								"INSERT forge::step_runs [{{ id: uuid::v4(\"{step_run_id}\"), \
								 run_id: uuid::v4(\"{run_id}\"), step_id: uuid::v4(\"{step_id}\"), \
								 job_run_id: uuid::v4(\"{job_run_id}\"), status: \"pending\" }}]"
							),
							Params::None,
						)?;
					}
				}
			}
		}

		Ok(Columns::single_row([("run_id", Value::Utf8(run_id.to_string()))]))
	}
}
