// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicBool, AtomicUsize, Ordering},
};

use reifydb::{
	Clock, IdentityId,
	auth::{
		registry::AuthenticationRegistry,
		service::{AuthEngine, AuthService, AuthServiceConfig},
	},
	catalog::catalog::Catalog,
	engine::engine::StandardEngine,
	runtime::context::rng::Rng,
	testing::db::TestDb,
	transaction::transaction::{admin::AdminTransaction, query::QueryTransaction},
	value::error::{Diagnostic, Error},
};
use reifydb_test_harness::{
	auth::{AuthResponseAssert, password_credentials},
	engine::AsEngine,
};

struct FaultyEngine {
	inner: StandardEngine,
	fail_admin: AtomicBool,
	fail_query: AtomicBool,
	healthy_queries: AtomicUsize,
}

impl FaultyEngine {
	fn new(inner: StandardEngine) -> Self {
		// This trait is the auth service's only route to storage, so it is the one inducible failure point.
		Self {
			inner,
			fail_admin: AtomicBool::new(false),
			fail_query: AtomicBool::new(false),
			healthy_queries: AtomicUsize::new(0),
		}
	}
}

impl AuthEngine for FaultyEngine {
	fn begin_admin(&self) -> Result<AdminTransaction, Error> {
		if self.fail_admin.load(Ordering::SeqCst) {
			return Err(storage_error());
		}
		AuthEngine::begin_admin(&self.inner)
	}

	fn begin_query(&self) -> Result<QueryTransaction, Error> {
		if self.fail_query.load(Ordering::SeqCst) {
			if self.healthy_queries.load(Ordering::SeqCst) == 0 {
				return Err(storage_error());
			}
			self.healthy_queries.fetch_sub(1, Ordering::SeqCst);
		}
		AuthEngine::begin_query(&self.inner)
	}

	fn catalog(&self) -> Catalog {
		AuthEngine::catalog(&self.inner)
	}
}

fn storage_error() -> Error {
	// A distinctive code so the assertions prove the original diagnostic survived the trip out.
	Error(Box::new(Diagnostic {
		code: "TEST_STORAGE".to_string(),
		message: "storage is unavailable".to_string(),
		..Default::default()
	}))
}

fn faulty_service(db: &TestDb) -> (Arc<FaultyEngine>, AuthService) {
	let engine = Arc::new(FaultyEngine::new(db.engine().clone()));
	let service = AuthService::new(
		engine.clone(),
		Arc::new(AuthenticationRegistry::default()),
		Rng::seeded(42),
		Clock::Real,
		AuthServiceConfig::default(),
	);
	(engine, service)
}

fn setup_user_and_login(db: &TestDb) -> (IdentityId, String) {
	db.admin("CREATE USER alice");
	db.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }");

	db.auth_service()
		.authenticate("password", password_credentials("alice", "alice-pass"))
		.unwrap()
		.expect_authenticated()
}

#[test]
fn revoke_token_surfaces_a_failed_drop() {
	// A logout that cannot delete the session row must not answer as if the token were unknown.
	let mut db = TestDb::memory();
	let (_identity, token) = setup_user_and_login(&db);
	let (engine, service) = faulty_service(&db);

	engine.fail_admin.store(true, Ordering::SeqCst);

	let err = service.revoke_token(&token).expect_err("a failed drop must reach the caller");
	assert_eq!(err.code, "TEST_STORAGE", "the storage diagnostic must survive, not be replaced");
	assert!(
		db.auth_service().validate_token(&token).unwrap().is_some(),
		"the session is still live after the failure"
	);

	db.stop();
}

#[test]
fn revoke_all_surfaces_a_failed_sweep() {
	// Invalidating every session for an identity must fail loudly, or live tokens survive a reset.
	let mut db = TestDb::memory();
	let (identity, _token) = setup_user_and_login(&db);
	let (engine, service) = faulty_service(&db);

	engine.fail_admin.store(true, Ordering::SeqCst);

	let err = service.revoke_all(identity).expect_err("a failed sweep must reach the caller");
	assert_eq!(err.code, "TEST_STORAGE", "the storage diagnostic must survive, not be replaced");

	db.stop();
}

#[test]
fn cleanup_expired_surfaces_a_failed_sweep() {
	// The scheduled cleanup reports success either way today, so a broken sweep is never noticed.
	let mut db = TestDb::memory();
	let (engine, service) = faulty_service(&db);

	engine.fail_admin.store(true, Ordering::SeqCst);

	let err = service.cleanup_expired().expect_err("a failed cleanup must reach the caller");
	assert_eq!(err.code, "TEST_STORAGE", "the storage diagnostic must survive, not be replaced");

	db.stop();
}

#[test]
fn validate_token_surfaces_a_failed_session_lookup() {
	// A storage blip must reject as an error, never look identical to a token that was never issued.
	let mut db = TestDb::memory();
	let (_identity, token) = setup_user_and_login(&db);
	let (engine, service) = faulty_service(&db);

	engine.fail_query.store(true, Ordering::SeqCst);

	let err = service.validate_token(&token).expect_err("a failed lookup must reach the caller");
	assert_eq!(err.code, "TEST_STORAGE", "the storage diagnostic must survive, not be replaced");

	db.stop();
}

#[test]
fn validate_token_surfaces_a_failed_catalog_token_lookup() {
	// The catalog-token fallback opens its own transaction; its failure must reject the request too.
	let mut db = TestDb::memory();
	let (engine, service) = faulty_service(&db);

	engine.healthy_queries.store(1, Ordering::SeqCst);
	engine.fail_query.store(true, Ordering::SeqCst);

	let err =
		service.validate_token("not-a-session-token").expect_err("the fallback failure must reach the caller");
	assert_eq!(err.code, "TEST_STORAGE", "the storage diagnostic must survive, not be replaced");

	db.stop();
}

#[test]
fn revoke_token_surfaces_a_failed_lookup() {
	// A revoke that cannot even read the token back must not answer as if the token were unknown.
	let mut db = TestDb::memory();
	let (_identity, token) = setup_user_and_login(&db);
	let (engine, service) = faulty_service(&db);

	engine.fail_query.store(true, Ordering::SeqCst);

	let err = service.revoke_token(&token).expect_err("a failed lookup must reach the caller");
	assert_eq!(err.code, "TEST_STORAGE", "the storage diagnostic must survive, not be replaced");

	db.stop();
}
