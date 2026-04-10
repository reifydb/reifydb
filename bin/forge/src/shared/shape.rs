// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::Migration;

pub fn migrations() -> Vec<Migration> {
	vec![
		Migration::new("001_create_forge_namespace", vec!["CREATE NAMESPACE forge;"]),
		Migration::new(
			"002_create_pipelines",
			vec!["CREATE TABLE forge::pipelines { \
				id: Uuid4, \
				name: Utf8, \
				description: Utf8, \
				created_at: DateTime, \
				updated_at: DateTime \
			};"],
		),
		Migration::new(
			"003_create_jobs",
			vec!["CREATE TABLE forge::jobs { \
				id: Uuid4, \
				pipeline_id: Uuid4, \
				name: Utf8, \
				position: Int4, \
				created_at: DateTime \
			};"],
		),
		Migration::new(
			"004_create_steps",
			vec!["CREATE TABLE forge::steps { \
				id: Uuid4, \
				job_id: Uuid4, \
				name: Utf8, \
				position: Int4, \
				command: Utf8, \
				timeout_seconds: Int4, \
				created_at: DateTime \
			};"],
		),
		Migration::new(
			"005_create_runs",
			vec!["CREATE TABLE forge::runs { \
				id: Uuid4, \
				pipeline_id: Uuid4, \
				status: Utf8, \
				triggered_by: Utf8, \
				started_at: DateTime, \
				finished_at: Option(DateTime) \
			};"],
		),
		Migration::new(
			"006_create_job_runs",
			vec!["CREATE TABLE forge::job_runs { \
				id: Uuid4, \
				run_id: Uuid4, \
				job_id: Uuid4, \
				status: Utf8, \
				started_at: Option(DateTime), \
				finished_at: Option(DateTime) \
			};"],
		),
		Migration::new(
			"007_create_step_runs",
			vec!["CREATE TABLE forge::step_runs { \
				id: Uuid4, \
				run_id: Uuid4, \
				step_id: Uuid4, \
				job_run_id: Uuid4, \
				status: Utf8, \
				started_at: Option(DateTime), \
				finished_at: Option(DateTime), \
				exit_code: Option(Int4) \
			};"],
		),
		Migration::new(
			"008_create_logs",
			vec!["CREATE TABLE forge::logs { \
				id: Uuid4, \
				run_id: Uuid4, \
				step_run_id: Uuid4, \
				stream: Utf8, \
				line: Utf8, \
				timestamp: DateTime, \
				line_number: Int4 \
			};"],
		),
		Migration::new(
			"009_create_artifacts",
			vec!["CREATE TABLE forge::artifacts { \
				id: Uuid4, \
				run_id: Uuid4, \
				step_run_id: Option(Uuid4), \
				name: Utf8, \
				content_type: Utf8, \
				size_bytes: Int8, \
				data: Blob, \
				created_at: DateTime \
			};"],
		),
		Migration::new(
			"010_create_job_dependencies",
			vec!["CREATE TABLE forge::job_dependencies { \
				id: Uuid4, \
				job_id: Uuid4, \
				depends_on_job_id: Uuid4 \
			};"],
		),
	]
}
