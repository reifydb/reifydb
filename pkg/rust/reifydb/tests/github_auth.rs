// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb::{
	Clock, Database, IdentityId,
	auth::{
		error::GithubError,
		github::{GithubApi, GithubConfig, GithubUser},
		registry::AuthenticationRegistry,
		service::{AuthConfigurator, AuthResponse, AuthService, AuthServiceConfig},
	},
	embedded,
	runtime::context::rng::Rng,
	transaction::transaction::Transaction,
	value::{Result, value::Value},
};

struct StubGithubApi {
	user: GithubUser,
	fail_exchange: bool,
}

impl GithubApi for StubGithubApi {
	fn exchange_code(&self, _config: &GithubConfig, code: &str) -> Result<String> {
		if self.fail_exchange {
			return Err(GithubError::ExchangeFailed {
				reason: "stub exchange failure".to_string(),
			}
			.into());
		}
		assert_eq!(code, "good-code", "the oauth code from the client must reach the code exchange verbatim");
		Ok("stub-access-token".to_string())
	}

	fn fetch_user(&self, access_token: &str) -> Result<GithubUser> {
		assert_eq!(
			access_token, "stub-access-token",
			"the user fetch must use the token minted by the code exchange"
		);
		Ok(self.user.clone())
	}
}

fn octocat() -> GithubUser {
	GithubUser {
		id: 583231,
		login: "octocat".to_string(),
	}
}

fn github_config() -> GithubConfig {
	GithubConfig {
		client_id: "test-client-id".to_string(),
		client_secret: "test-client-secret".to_string(),
		redirect_uri: "http://localhost:8080/auth/github/callback".to_string(),
	}
}

fn service_with(db: &Database, config: AuthServiceConfig, api: StubGithubApi) -> AuthService {
	AuthService::with_github_api(
		Arc::new(db.engine().clone()),
		Arc::new(AuthenticationRegistry::default()),
		Rng::seeded(42),
		Clock::Real,
		config,
		Arc::new(api),
	)
}

fn configured_service(db: &Database, api: StubGithubApi) -> AuthService {
	service_with(db, AuthConfigurator::new().github(github_config()).configure(), api)
}

fn begin_login(service: &AuthService) -> (String, HashMap<String, String>) {
	match service.authenticate("github", HashMap::new()).unwrap() {
		AuthResponse::Challenge {
			challenge_id,
			payload,
		} => (challenge_id, payload),
		other => panic!("expected a challenge to start the oauth flow, got {:?}", other),
	}
}

fn complete_login(service: &AuthService, challenge_id: &str, state: &str, code: &str) -> AuthResponse {
	service.authenticate(
		"github",
		HashMap::from([
			("challenge_id".to_string(), challenge_id.to_string()),
			("state".to_string(), state.to_string()),
			("code".to_string(), code.to_string()),
		]),
	)
	.unwrap()
}

fn full_login(service: &AuthService) -> AuthResponse {
	let (challenge_id, payload) = begin_login(service);
	complete_login(service, &challenge_id, payload.get("state").unwrap(), "good-code")
}

fn find_github_identity_id(db: &Database, user_id: &str) -> Option<IdentityId> {
	let engine = db.engine();
	let mut txn = engine.begin_query(IdentityId::root()).unwrap();
	engine.catalog()
		.find_identity_by_attribute_value(
			&mut Transaction::Query(&mut txn),
			"github_user_id",
			&Value::Utf8(user_id.to_string()),
		)
		.unwrap()
		.map(|ident| ident.id)
}

fn attribute_value(db: &Database, identity: IdentityId, name: &str) -> Option<Value> {
	let engine = db.engine();
	let mut txn = engine.begin_query(IdentityId::root()).unwrap();
	let catalog = engine.catalog();
	let attribute = catalog.find_identity_attribute_by_name(&mut Transaction::Query(&mut txn), name).unwrap()?;
	catalog.find_identity_attribute_values(&mut Transaction::Query(&mut txn), identity)
		.unwrap()
		.into_iter()
		.find(|v| v.attribute == attribute.id)
		.map(|v| v.value)
}

#[test]
fn test_begin_returns_authorize_url_and_state() {
	let db = embedded::memory().build().unwrap();
	let service = configured_service(
		&db,
		StubGithubApi {
			user: octocat(),
			fail_exchange: false,
		},
	);

	let (_, payload) = begin_login(&service);

	// The client navigates to authorize_url and later echoes state back; both must
	// be present or the flow cannot round-trip through github.
	let authorize_url = payload.get("authorize_url").unwrap();
	assert!(authorize_url.starts_with("https://github.com/login/oauth/authorize?"));
	assert!(authorize_url.contains("client_id=test%2Dclient%2Did"));

	let state = payload.get("state").unwrap();
	// 32 bytes of entropy as hex; anything shorter is guessable and defeats CSRF protection.
	assert_eq!(state.len(), 64);
	assert!(state.bytes().all(|b| b.is_ascii_hexdigit()));
	assert!(authorize_url.contains(state));
}

#[test]
fn test_complete_login_provisions_identity_and_mints_token() {
	let db = embedded::memory().build().unwrap();
	let service = configured_service(
		&db,
		StubGithubApi {
			user: octocat(),
			fail_exchange: false,
		},
	);

	assert!(find_github_identity_id(&db, "583231").is_none());

	let response = full_login(&service);
	let AuthResponse::Authenticated {
		identity,
		token,
	} = response
	else {
		panic!("expected successful authentication, got {:?}", response);
	};

	assert_ne!(identity, IdentityId::default());
	assert!(!token.is_empty());

	// The auto-provisioned identity must be resolvable by the immutable github
	// account id, not just by name, so later logins find it again.
	assert_eq!(find_github_identity_id(&db, "583231"), Some(identity));

	// The session token must actually validate, otherwise the client is handed a
	// token that every subsequent request rejects.
	let validated = service.validate_token(&token).expect("minted token must validate");
	assert_eq!(validated.identity, identity);
}

#[test]
fn test_second_login_reuses_identity() {
	let db = embedded::memory().build().unwrap();
	let service = configured_service(
		&db,
		StubGithubApi {
			user: octocat(),
			fail_exchange: false,
		},
	);

	let AuthResponse::Authenticated {
		identity: first,
		..
	} = full_login(&service)
	else {
		panic!("first login must authenticate");
	};
	let AuthResponse::Authenticated {
		identity: second,
		..
	} = full_login(&service)
	else {
		panic!("second login must authenticate");
	};

	// A returning github user must land on the same account; a fresh identity per
	// login would orphan all their data.
	assert_eq!(first, second);
}

#[test]
fn test_tampered_state_fails() {
	let db = embedded::memory().build().unwrap();
	let service = configured_service(
		&db,
		StubGithubApi {
			user: octocat(),
			fail_exchange: false,
		},
	);

	let (challenge_id, _) = begin_login(&service);

	// A state that does not match the challenge is a CSRF/code-injection attempt:
	// the attacker supplies their own code but cannot know the victim's state.
	let response = complete_login(&service, &challenge_id, "attacker-forged-state", "good-code");
	let AuthResponse::Failed {
		reason,
	} = response
	else {
		panic!("expected failure for tampered state, got {:?}", response);
	};
	assert_eq!(reason, "invalid oauth state");
	assert!(find_github_identity_id(&db, "583231").is_none());
}

#[test]
fn test_replayed_challenge_fails() {
	let db = embedded::memory().build().unwrap();
	let service = configured_service(
		&db,
		StubGithubApi {
			user: octocat(),
			fail_exchange: false,
		},
	);

	let (challenge_id, payload) = begin_login(&service);
	let state = payload.get("state").unwrap();

	let first = complete_login(&service, &challenge_id, state, "good-code");
	assert!(matches!(first, AuthResponse::Authenticated { .. }), "first completion must succeed");

	// The challenge is one-time: replaying the same challenge_id (e.g. a stolen
	// callback URL) must not mint a second session.
	let replay = complete_login(&service, &challenge_id, state, "good-code");
	let AuthResponse::Failed {
		reason,
	} = replay
	else {
		panic!("expected replay to fail, got {:?}", replay);
	};
	assert_eq!(reason, "invalid or expired challenge");
}

#[test]
fn test_unconfigured_github_fails() {
	let db = embedded::memory().build().unwrap();
	let service = service_with(
		&db,
		AuthConfigurator::new().configure(),
		StubGithubApi {
			user: octocat(),
			fail_exchange: false,
		},
	);

	let response = service.authenticate("github", HashMap::new()).unwrap();
	let AuthResponse::Failed {
		reason,
	} = response
	else {
		panic!("expected failure without github config, got {:?}", response);
	};
	assert_eq!(reason, "github authentication is not configured");
}

#[test]
fn test_exchange_failure_fails_login_without_error() {
	let db = embedded::memory().build().unwrap();
	let service = configured_service(
		&db,
		StubGithubApi {
			user: octocat(),
			fail_exchange: true,
		},
	);

	// A github outage must surface as a failed login, not as a transport-level
	// error, and must not leave a half-provisioned identity behind.
	let response = full_login(&service);
	let AuthResponse::Failed {
		reason,
	} = response
	else {
		panic!("expected failure when the code exchange fails, got {:?}", response);
	};
	assert_eq!(reason, "github verification failed");
	assert!(find_github_identity_id(&db, "583231").is_none());
}

#[test]
fn test_github_login_attribute_recorded_when_declared() {
	let db = embedded::memory().build().unwrap();
	db.admin_as_root("create user attribute github_login: utf8", ()).unwrap();

	let service = configured_service(
		&db,
		StubGithubApi {
			user: octocat(),
			fail_exchange: false,
		},
	);

	let AuthResponse::Authenticated {
		identity,
		..
	} = full_login(&service)
	else {
		panic!("login must authenticate");
	};

	// The declared attribute is the embedder's opt-in to display the github login
	// in their UI; after sign-in it must hold the fetched login name, alongside
	// the auto-managed github_user_id lookup attribute.
	assert_eq!(attribute_value(&db, identity, "github_login"), Some(Value::Utf8("octocat".to_string())));
	assert_eq!(attribute_value(&db, identity, "github_user_id"), Some(Value::Utf8("583231".to_string())));
}

#[test]
fn test_login_succeeds_without_declared_attribute() {
	let db = embedded::memory().build().unwrap();
	let service = configured_service(
		&db,
		StubGithubApi {
			user: octocat(),
			fail_exchange: false,
		},
	);

	// Embedders that never declared github_login must still get working logins;
	// the display attribute is strictly opt-in. Only the github_user_id lookup
	// attribute is auto-managed, because identity resolution depends on it.
	let AuthResponse::Authenticated {
		identity,
		..
	} = full_login(&service)
	else {
		panic!("login must authenticate without the attribute declared");
	};

	assert_eq!(attribute_value(&db, identity, "github_login"), None);
	assert_eq!(attribute_value(&db, identity, "github_user_id"), Some(Value::Utf8("583231".to_string())));
}
