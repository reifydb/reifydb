// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fs;

use reifydb_client::GrpcClient;
use reifydb_type::{error::Error, value::Value};
use tracing::{error, info};
use uuid::Uuid;

/// Execute an entire job: claim the job_run, run all steps sequentially, then complete.
pub async fn execute_job(client: &GrpcClient, job_run_id: &str, job_id: &str, run_id: &str) -> Result<(), Error> {
	// Create workspace directory for this run
	let workspace = format!("/tmp/forge-runs/{run_id}");
	fs::create_dir_all(&workspace).ok();

	// 1. Claim: set job_run status to running (only if still pending)
	client.command(
		&format!("UPDATE forge::job_runs {{ status: \"running\", started_at: datetime::now() }} \
				 FILTER id == uuid::v4(\"{job_run_id}\") AND status == \"pending\""),
		None,
	)
	.await?;

	// 2. Query steps for this job, ordered by position
	let steps_result = client
		.query(
			&format!("FROM forge::steps | FILTER job_id == uuid::v4(\"{job_id}\") | SORT {{position:ASC}}"),
			None,
		)
		.await?;

	// Collect step IDs upfront so the non-Send FrameRows iterator doesn't live across awaits
	let step_ids: Vec<String> = steps_result
		.frames
		.first()
		.map(|f| f.rows().filter_map(|row| row.get_value("id").map(|v| v.to_string())).collect())
		.unwrap_or_default();

	let mut final_status = "succeeded";

	// 3. Execute each step sequentially
	for step_id in &step_ids {
		// Find the step_run for this step + job_run
		let step_run_result = client
			.query(
				&format!(
					"FROM forge::step_runs | FILTER job_run_id == uuid::v4(\"{job_run_id}\") AND step_id == uuid::v4(\"{step_id}\")"
				),
				None,
			)
			.await?;

		let step_run_id = match step_run_result
			.frames
			.first()
			.and_then(|f| f.rows().next())
			.and_then(|row| row.get_value("id").map(|v| v.to_string()))
		{
			Some(id) => id,
			None => continue,
		};

		// Execute the step
		let step_succeeded = execute_step(client, &step_run_id, step_id, run_id, &workspace).await?;

		if !step_succeeded {
			final_status = "failed";
			// Skip remaining step_runs in this job_run
			client.command(
				&format!("UPDATE forge::step_runs {{ status: \"skipped\" }} \
					 FILTER job_run_id == uuid::v4(\"{job_run_id}\") AND status == \"pending\""),
				None,
			)
			.await?;
			break;
		}
	}

	// 4. Complete the job_run via the procedure (handles cascading)
	client.command(&format!("CALL forge::complete_job_run(uuid::v4(\"{job_run_id}\"), \"{final_status}\")"), None)
		.await?;

	info!("Job run {} completed with status: {}", job_run_id, final_status);
	Ok(())
}

/// Execute a single step: update status, run command, store logs, report result.
/// Returns true if succeeded, false if failed.
async fn execute_step(
	client: &GrpcClient,
	step_run_id: &str,
	step_id: &str,
	run_id: &str,
	workspace: &str,
) -> Result<bool, Error> {
	// Update step_run to running
	client.command(
		&format!("UPDATE forge::step_runs {{ status: \"running\", started_at: datetime::now() }} \
				 FILTER id == uuid::v4(\"{step_run_id}\")"),
		None,
	)
	.await?;

	// Read the step definition to get the command
	let step_result =
		client.query(&format!("FROM forge::steps | FILTER id == uuid::v4(\"{step_id}\")"), None).await?;

	let command_str = step_result
		.frames
		.first()
		.and_then(|f| f.rows().next().and_then(|row| row.get_value("command").map(|v| v.to_string())))
		.unwrap_or_default();

	if command_str.is_empty() {
		error!("Step run {} failed: no command found for step", step_run_id);
		client.command(
			&format!(
				"UPDATE forge::step_runs {{ status: \"failed\", finished_at: datetime::now(), exit_code: -1 }} \
					 FILTER id == uuid::v4(\"{step_run_id}\")"
			),
			None,
		)
		.await?;
		return Ok(false);
	}

	let is_exec_call = command_str.starts_with("CALL forge::exec(");

	// If command is a CALL to forge::exec, inject the workspace path
	let rql = if is_exec_call {
		// Create step-specific workspace subdirectory
		let step_workspace = format!("{workspace}/{step_run_id}");
		fs::create_dir_all(&step_workspace).ok();

		// CALL forge::exec("cargo check") → CALL forge::exec("cargo check",
		// "/tmp/forge-runs/{run_id}/{step_run_id}")
		command_str.replacen(")", &format!(", \"{step_workspace}\")"), 1)
	} else {
		command_str.clone()
	};

	info!("Executing RQL for step_run {}: {}", step_run_id, rql);

	// Execute the RQL statement
	let result = client.command(&rql, None).await;

	match result {
		Ok(command_result) => {
			if is_exec_call {
				let step_workspace = format!("{workspace}/{step_run_id}");

				// Read exit_code from result frames
				let exit_code = command_result
					.frames
					.first()
					.and_then(|f| f.rows().next())
					.and_then(|row| row.get_value("exit_code"))
					.and_then(|v| match v {
						Value::Int4(c) => Some(c),
						_ => None,
					})
					.unwrap_or(-1);

				let mut line_number: i32 = 1;

				// Read and insert stdout
				if let Ok(stdout) = fs::read_to_string(format!("{step_workspace}/stdout.log")) {
					for line in stdout.lines() {
						let line_escaped = line.replace('"', "\\\"");
						let log_id = Uuid::new_v4();
						let _ = client
							.command(
								&format!(
									"INSERT forge::logs [{{ id: uuid::v4(\"{log_id}\"), run_id: uuid::v4(\"{run_id}\"), \
									 step_run_id: uuid::v4(\"{step_run_id}\"), stream: \"stdout\", \
									 line: \"{line_escaped}\", timestamp: datetime::now(), line_number: {line_number} }}]"
								),
								None,
							)
							.await;
						line_number += 1;
					}
				}

				// Read and insert stderr
				if let Ok(stderr) = fs::read_to_string(format!("{step_workspace}/stderr.log")) {
					for line in stderr.lines() {
						let line_escaped = line.replace('"', "\\\"");
						let log_id = Uuid::new_v4();
						let _ = client
							.command(
								&format!(
									"INSERT forge::logs [{{ id: uuid::v4(\"{log_id}\"), run_id: uuid::v4(\"{run_id}\"), \
									 step_run_id: uuid::v4(\"{step_run_id}\"), stream: \"stderr\", \
									 line: \"{line_escaped}\", timestamp: datetime::now(), line_number: {line_number} }}]"
								),
								None,
							)
							.await;
						line_number += 1;
					}
				}

				let succeeded = exit_code == 0;
				let status = if succeeded {
					"succeeded"
				} else {
					"failed"
				};

				client.command(
					&format!(
						"UPDATE forge::step_runs {{ status: \"{status}\", finished_at: datetime::now(), \
							 exit_code: {exit_code} }} FILTER id == uuid::v4(\"{step_run_id}\")"
					),
					None,
				)
				.await?;

				info!("Step run {} {} (exit_code: {})", step_run_id, status, exit_code);
				Ok(succeeded)
			} else {
				// Pure RQL step — collect log lines from result frames
				let mut log_lines: Vec<String> = Vec::new();
				for frame in &command_result.frames {
					let headers: Vec<&str> =
						frame.columns.iter().map(|c| c.name.as_str()).collect();
					log_lines.push(headers.join(" | "));
					for row in frame.rows() {
						let values: Vec<String> = headers
							.iter()
							.map(|col| {
								row.get_value(col)
									.map(|v| v.to_string())
									.unwrap_or_default()
							})
							.collect();
						log_lines.push(values.join(" | "));
					}
				}

				for (i, line) in log_lines.iter().enumerate() {
					let line_escaped = line.replace('"', "\\\"");
					let log_id = Uuid::new_v4();
					let line_number = (i as i32) + 1;
					let _ = client
						.command(
							&format!(
								"INSERT forge::logs [{{ id: uuid::v4(\"{log_id}\"), run_id: uuid::v4(\"{run_id}\"), \
								 step_run_id: uuid::v4(\"{step_run_id}\"), stream: \"stdout\", \
								 line: \"{line_escaped}\", timestamp: datetime::now(), line_number: {line_number} }}]"
							),
							None,
						)
						.await;
				}

				client.command(
					&format!(
						"UPDATE forge::step_runs {{ status: \"succeeded\", finished_at: datetime::now(), \
							 exit_code: 0 }} FILTER id == uuid::v4(\"{step_run_id}\")"
					),
					None,
				)
				.await?;
				info!("Step run {} succeeded", step_run_id);
				Ok(true)
			}
		}
		Err(e) => {
			// Log the error diagnostic as stderr
			let err_msg = e.message.replace('"', "\\\"");
			let log_id = Uuid::new_v4();
			let _ = client
				.command(
					&format!(
						"INSERT forge::logs [{{ id: uuid::v4(\"{log_id}\"), run_id: uuid::v4(\"{run_id}\"), \
						 step_run_id: uuid::v4(\"{step_run_id}\"), stream: \"stderr\", \
						 line: \"{err_msg}\", timestamp: datetime::now(), line_number: 1 }}]"
					),
					None,
				)
				.await;

			error!("Step run {} failed: {}", step_run_id, e.message);
			client.command(
				&format!(
					"UPDATE forge::step_runs {{ status: \"failed\", finished_at: datetime::now(), exit_code: 1 }} \
						 FILTER id == uuid::v4(\"{step_run_id}\")"
				),
				None,
			)
			.await?;
			Ok(false)
		}
	}
}
