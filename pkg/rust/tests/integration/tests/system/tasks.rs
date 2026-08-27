// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	Clock, IdentityId, MockClock, RuntimeConfig,
	catalog::find_token_by_value,
	embedded,
	sub_task::{
		context::TaskContext,
		task::{TaskExecutor, TaskWork},
	},
	system::tasks::create_system_tasks,
	testing::db::TestDb,
	transaction::transaction::Transaction,
	value::value::duration::Duration,
};
use reifydb_test_harness::auth::{AuthResponseAssert, password_credentials};

const AUTH_CLEANUP: &str = "auth-cleanup";

fn db_with_session_ttl(mock: &MockClock, session_ttl: Duration) -> TestDb {
	let mut config = RuntimeConfig::default();
	config.clock = Clock::Mock(mock.clone());

	TestDb::from(
		embedded::memory()
			.with_runtime_config(config)
			.with_auth(move |a| a.session_ttl(session_ttl))
			.build()
			.unwrap(),
	)
}

fn login(db: &TestDb) -> String {
	db.admin("CREATE USER alice");
	db.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }");

	db.auth_service()
		.authenticate("password", password_credentials("alice", "alice-pass"))
		.unwrap()
		.expect_authenticated()
		.1
}

fn token_row_exists(db: &TestDb, token: &str) -> bool {
	let mut txn = db.engine().begin_query(IdentityId::system()).unwrap();
	find_token_by_value(&mut Transaction::Query(&mut txn), token).unwrap().is_some()
}

#[test]
fn test_auth_cleanup_task_is_registered() {
	// expired tokens and challenges accumulate forever unless the scheduler is handed this task
	let mut db = TestDb::memory();

	let tasks = create_system_tasks(db.engine().ioc()).unwrap();
	let task = tasks.iter().find(|task| task.name == AUTH_CLEANUP);

	assert!(
		task.is_some(),
		"no {} task in {:?}",
		AUTH_CLEANUP,
		tasks.iter().map(|task| task.name.as_str()).collect::<Vec<_>>()
	);
	// the sweep is synchronous, so a tokio executor would park a reactor thread on it
	assert_eq!(task.unwrap().executor, TaskExecutor::ComputePool);

	db.stop();
}

#[test]
fn test_auth_cleanup_task_evicts_expired_token() {
	// registration is worthless unless invoking the task actually drops the expired row
	let mock = MockClock::from_millis(1_700_000_000_000);
	let mut db = db_with_session_ttl(&mock, Duration::from_seconds(60).unwrap());

	let token = login(&db);
	assert!(token_row_exists(&db, &token), "token row should exist right after login");

	mock.advance_secs(61);
	assert!(token_row_exists(&db, &token), "an expired token row survives until something sweeps it");

	let tasks = create_system_tasks(db.engine().ioc()).unwrap();
	let task = tasks.into_iter().find(|task| task.name == AUTH_CLEANUP).expect("auth cleanup task");
	let TaskWork::Sync(work) = task.work else {
		panic!("auth cleanup work must be sync");
	};

	work(TaskContext::new(db.engine().clone())).unwrap();

	assert!(!token_row_exists(&db, &token), "auth cleanup should have dropped the expired token row");

	db.stop();
}
