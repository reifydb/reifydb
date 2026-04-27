// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::LazyLock;

use reifydb_core::value::column::columns::Columns;
use reifydb_routine::routine::{Routine, RoutineInfo, context::ProcedureContext, error::RoutineError};
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, r#type::Type},
};
use uuid::Uuid;

static INFO: LazyLock<RoutineInfo> = LazyLock::new(|| RoutineInfo::new("forge::run_pipeline"));

/// Creates a new pipeline run with job_runs and step_runs for every job/step in the pipeline.
///
/// Expects 1 positional argument: pipeline_id (Uuid4).
///
/// Jobs with no dependencies get job_runs with status "pending" (immediately claimable).
/// Jobs with dependencies get job_runs with status "blocked" (waiting on deps).
pub struct RunPipelineProcedure;

impl RunPipelineProcedure {
	pub fn new() -> Self {
		Self
	}
}

impl Default for RunPipelineProcedure {
	fn default() -> Self {
		Self::new()
	}
}

impl<'a, 'tx> Routine<ProcedureContext<'a, 'tx>> for RunPipelineProcedure {
	fn info(&self) -> &RoutineInfo {
		&INFO
	}
	fn return_type(&self, _input_types: &[Type]) -> Type {
		Type::Any
	}
	fn execute(&self, ctx: &mut ProcedureContext<'a, 'tx>, _args: &Columns) -> Result<Columns, RoutineError> {
		let pipeline_id = match ctx.params {
			Params::Positional(args) if !args.is_empty() => args[0].clone(),
			Params::Positional(args) => {
				return Err(RoutineError::ProcedureArityMismatch {
					procedure: Fragment::internal("forge::run_pipeline"),
					expected: 1,
					actual: args.len(),
				});
			}
			_ => {
				return Err(RoutineError::ProcedureArityMismatch {
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
				return Err(RoutineError::ProcedureInvalidArgumentType {
					procedure: Fragment::internal("forge::run_pipeline"),
					argument_index: 0,
					expected: vec![Type::Uuid4, Type::Utf8],
					actual: pipeline_id.get_type(),
				});
			}
		};

		// Validate pipeline exists
		let pipelines =
			ctx.tx.rql(
				&format!("FROM forge::pipelines | FILTER id == uuid::v4(\"{pipeline_id_str}\")"),
				Params::None,
			)
			.check()?;
		if pipelines.is_empty() || pipelines[0].rows().next().is_none() {
			return Err(RoutineError::ProcedureExecutionFailed {
				procedure: Fragment::internal("forge::run_pipeline"),
				reason: format!("Pipeline not found: {}", pipeline_id_str),
			});
		}

		// Create the run
		let run_id = Uuid::new_v4();
		ctx.tx.rql(
			&format!(
				"INSERT forge::runs [{{ id: uuid::v4(\"{run_id}\"), pipeline_id: uuid::v4(\"{pipeline_id_str}\"), \
				 status: \"pending\", triggered_by: \"manual\", started_at: datetime::now() }}]"
			),
			Params::None,
		)
		.check()?;

		// Query all jobs for this pipeline
		let jobs =
			ctx.tx.rql(
				&format!(
					"FROM forge::jobs | FILTER pipeline_id == uuid::v4(\"{pipeline_id_str}\") | SORT {{position:ASC}}"
				),
				Params::None,
			)
			.check()?;

		if let Some(job_frame) = jobs.first() {
			for job_row in job_frame.rows() {
				let job_id = job_row.get_value("id").map(|v| v.to_string()).ok_or_else(|| {
					RoutineError::ProcedureExecutionFailed {
						procedure: Fragment::internal("forge::run_pipeline"),
						reason: "jobs row is missing required field 'id'".to_string(),
					}
				})?;

				// Check if this job has any dependencies
				let deps =
					ctx.tx.rql(
						&format!(
							"FROM forge::job_dependencies | FILTER job_id == uuid::v4(\"{job_id}\")"
						),
						Params::None,
					)
					.check()?;

				let has_deps = deps.first().is_some_and(|f| f.rows().next().is_some());
				let job_run_status = if has_deps {
					"blocked"
				} else {
					"pending"
				};

				// Create job_run
				let job_run_id = Uuid::new_v4();
				ctx.tx.rql(
					&format!("INSERT forge::job_runs [{{ id: uuid::v4(\"{job_run_id}\"), \
						 run_id: uuid::v4(\"{run_id}\"), job_id: uuid::v4(\"{job_id}\"), \
						 status: \"{job_run_status}\" }}]"),
					Params::None,
				)
				.check()?;

				// Query steps for this job and create step_runs
				let steps =
					ctx.tx.rql(
						&format!(
							"FROM forge::steps | FILTER job_id == uuid::v4(\"{job_id}\") | SORT {{position:ASC}}"
						),
						Params::None,
					)
					.check()?;

				if let Some(step_frame) = steps.first() {
					for step_row in step_frame.rows() {
						let step_id = step_row
							.get_value("id")
							.map(|v| v.to_string())
							.ok_or_else(|| RoutineError::ProcedureExecutionFailed {
								procedure: Fragment::internal("forge::run_pipeline"),
								reason: "steps row is missing required field 'id'"
									.to_string(),
							})?;
						let step_run_id = Uuid::new_v4();

						ctx.tx.rql(
							&format!(
								"INSERT forge::step_runs [{{ id: uuid::v4(\"{step_run_id}\"), \
								 run_id: uuid::v4(\"{run_id}\"), step_id: uuid::v4(\"{step_id}\"), \
								 job_run_id: uuid::v4(\"{job_run_id}\"), status: \"pending\" }}]"
							),
							Params::None,
						)
						.check()?;
					}
				}
			}
		}

		Ok(Columns::single_row([("run_id", Value::Utf8(run_id.to_string()))]))
	}
}
