// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::Database;
use reifydb_type::params::Params;

const PIPELINE_ID: &str = "00000000-0000-4000-a000-000000000001";
const JOB_CHECK_ID: &str = "00000000-0000-4000-a000-000000000002";
const STEP_CHECK_ID: &str = "00000000-0000-4000-a000-000000000003";
const STEP_TEST_ID: &str = "00000000-0000-4000-a000-000000000004";
const JOB_TEST_ID: &str = "00000000-0000-4000-a000-000000000005";
const JOB_DEP_TEST_CHECK_ID: &str = "00000000-0000-4000-a000-000000000006";

const DUMMY_PIPELINE_ID: &str = "00000000-0000-4000-a000-000000000010";
const DUMMY_JOB_ID: &str = "00000000-0000-4000-a000-000000000011";
const DUMMY_STEP_CHECK_ID: &str = "00000000-0000-4000-a000-000000000012";
const DUMMY_STEP_DATE_ID: &str = "00000000-0000-4000-a000-000000000013";

pub fn seed_default_pipeline(db: &Database) {
	// Check if the pipeline already exists
	let existing = db
		.admin_as_root(
			&format!("FROM forge::pipelines | FILTER id == uuid::v4(\"{PIPELINE_ID}\")"),
			Params::None,
		)
		.unwrap_or_default();

	if existing.first().is_some_and(|f| f.rows().next().is_some()) {
		tracing::debug!("Default 'reifydb' pipeline already exists, skipping seed");
		return;
	}

	tracing::info!("Seeding default 'reifydb' pipeline...");

	db.admin_as_root(
		&format!("INSERT forge::pipelines [{{ id: uuid::v4(\"{PIPELINE_ID}\"), \
			 name: \"reifydb\", description: \"Check and test the ReifyDB workspace\", \
			 created_at: datetime::now(), updated_at: datetime::now() }}]"),
		Params::None,
	)
	.expect("Failed to seed pipeline");

	db.admin_as_root(
		&format!("INSERT forge::jobs [{{ id: uuid::v4(\"{JOB_CHECK_ID}\"), \
			 pipeline_id: uuid::v4(\"{PIPELINE_ID}\"), name: \"check\", position: 1, \
			 created_at: datetime::now() }}]"),
		Params::None,
	)
	.expect("Failed to seed job");

	db.admin_as_root(
		&format!("INSERT forge::jobs [{{ id: uuid::v4(\"{JOB_TEST_ID}\"), \
			 pipeline_id: uuid::v4(\"{PIPELINE_ID}\"), name: \"test\", position: 2, \
			 created_at: datetime::now() }}]"),
		Params::None,
	)
	.expect("Failed to seed job");

	// Test job depends on check job
	db.admin_as_root(
		&format!("INSERT forge::job_dependencies [{{ id: uuid::v4(\"{JOB_DEP_TEST_CHECK_ID}\"), \
			 job_id: uuid::v4(\"{JOB_TEST_ID}\"), depends_on_job_id: uuid::v4(\"{JOB_CHECK_ID}\") }}]"),
		Params::None,
	)
	.expect("Failed to seed job dependency");

	db.admin_as_root(
		&format!("INSERT forge::steps [{{ id: uuid::v4(\"{STEP_CHECK_ID}\"), \
			 job_id: uuid::v4(\"{JOB_CHECK_ID}\"), name: \"cargo check\", position: 1, \
			 command: \"CALL forge::exec(\\\"cargo check\\\")\", timeout_seconds: 600, \
			 created_at: datetime::now() }}]"),
		Params::None,
	)
	.expect("Failed to seed step");

	db.admin_as_root(
		&format!("INSERT forge::steps [{{ id: uuid::v4(\"{STEP_TEST_ID}\"), \
			 job_id: uuid::v4(\"{JOB_TEST_ID}\"), name: \"cargo test\", position: 1, \
			 command: \"CALL forge::exec(\\\"cargo test\\\")\", timeout_seconds: 600, \
			 created_at: datetime::now() }}]"),
		Params::None,
	)
	.expect("Failed to seed step");

	tracing::info!("Default 'reifydb' pipeline seeded successfully");

	seed_dummy_pipeline(db);
}

pub fn seed_dummy_pipeline(db: &Database) {
	let existing = db
		.admin_as_root(
			&format!("FROM forge::pipelines | FILTER id == uuid::v4(\"{DUMMY_PIPELINE_ID}\")"),
			Params::None,
		)
		.unwrap_or_default();

	if existing.first().is_some_and(|f| f.rows().next().is_some()) {
		tracing::debug!("'dummy pipeline' already exists, skipping seed");
		return;
	}

	tracing::info!("Seeding 'dummy pipeline'...");

	db.admin_as_root(
		&format!("INSERT forge::pipelines [{{ id: uuid::v4(\"{DUMMY_PIPELINE_ID}\"), \
			 name: \"dummy pipeline\", description: \"Queries pipelines and its own jobs\", \
			 created_at: datetime::now(), updated_at: datetime::now() }}]"),
		Params::None,
	)
	.expect("Failed to seed dummy pipeline");

	db.admin_as_root(
		&format!("INSERT forge::jobs [{{ id: uuid::v4(\"{DUMMY_JOB_ID}\"), \
			 pipeline_id: uuid::v4(\"{DUMMY_PIPELINE_ID}\"), name: \"query-self\", position: 1, \
			 created_at: datetime::now() }}]"),
		Params::None,
	)
	.expect("Failed to seed dummy job");

	db.admin_as_root(
		&format!("INSERT forge::steps [{{ id: uuid::v4(\"{DUMMY_STEP_CHECK_ID}\"), \
			 job_id: uuid::v4(\"{DUMMY_JOB_ID}\"), name: \"list pipelines\", position: 1, \
			 command: \"FROM forge::pipelines\", timeout_seconds: 600, \
			 created_at: datetime::now() }}]"),
		Params::None,
	)
	.expect("Failed to seed dummy step 1");

	db.admin_as_root(
		&format!("INSERT forge::steps [{{ id: uuid::v4(\"{DUMMY_STEP_DATE_ID}\"), \
			 job_id: uuid::v4(\"{DUMMY_JOB_ID}\"), name: \"query own jobs\", position: 2, \
			 command: \"FROM forge::jobs | FILTER pipeline_id == uuid::v4(\\\"{DUMMY_PIPELINE_ID}\\\")\", timeout_seconds: 30, \
			 created_at: datetime::now() }}]"),
		Params::None,
	)
	.expect("Failed to seed dummy step 2");

	tracing::info!("'dummy pipeline' seeded successfully");
}
