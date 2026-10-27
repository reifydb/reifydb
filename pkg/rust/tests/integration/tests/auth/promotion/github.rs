// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb::{auth::service::AuthResponse, embedded, testing::db::TestDb};
use reifydb_test_harness::auth::{AuthResponseAssert, auth_service};
use reifydb_value::value::identity::IdentityKind;

use super::name_and_kind;
use crate::auth::github::{StubGithubApi, octocat};

const CLIENT_SECRET: &str = "test-client-secret";

#[test]
fn a_promotion_onto_a_taken_name_fails_and_leaves_the_guest_intact() {
	// A half-applied rename would leave a guest that is neither its old self nor the new account.
	let db = TestDb::from(embedded::memory().with_auth(|a| a.github_client_secret(CLIENT_SECRET)).build().unwrap());
	db.admin("CREATE AUTHENTICATION METHOD guest WITH { ttl: forever }");
	db.admin("CREATE AUTHENTICATION METHOD github WITH { ttl: forever, promote_guest: true, client_id: 'id', redirect_uri: 'http://app/cb' }");
	db.admin(&format!("CREATE USER `github:{}`", octocat().id));
	let service = auth_service(&db)
		.configure(|c| c.github_client_secret(CLIENT_SECRET))
		.github_api(Arc::new(StubGithubApi {
			user: octocat(),
			fail_exchange: false,
		}))
		.build();
	let (guest, guest_token) = service.authenticate("guest", HashMap::new()).unwrap().expect_authenticated();
	let guest_name = name_and_kind(&db, guest).0;

	let (challenge_id, state) = match service.authenticate_as(guest, "github", HashMap::new()).unwrap() {
		AuthResponse::Challenge {
			challenge_id,
			payload,
		} => (challenge_id, payload.get("state").unwrap().clone()),
		other => panic!("expected the oauth challenge, got {other:?}"),
	};
	let err = service
		.authenticate_as(
			guest,
			"github",
			HashMap::from([
				("challenge_id".to_string(), challenge_id),
				("state".to_string(), state),
				("code".to_string(), "good-code".to_string()),
			]),
		)
		.unwrap_err();

	assert_eq!(err.diagnostic().code, "CA_040");
	assert_eq!(name_and_kind(&db, guest), (guest_name, IdentityKind::Guest));
	assert_eq!(service.validate_token(&guest_token).unwrap().map(|s| s.identity), Some(guest));
}
