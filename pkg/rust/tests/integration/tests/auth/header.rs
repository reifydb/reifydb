// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	sub_server::auth::{AuthError, extract_identity_from_auth_header},
	testing::db::TestDb,
};
use reifydb_test_harness::auth::auth_service;

#[test]
fn an_unsupported_scheme_is_a_bad_header_never_a_bad_token() {
	// Basic is parsed today and then rejected as if the credentials were wrong. The server does
	// not speak it at all, so the honest answer names the header, not the credentials.
	// "Invalid token" tells the client to fetch fresh credentials and retry the same dead scheme.
	let db = TestDb::memory();
	let service = auth_service(&db).build();

	let basic = extract_identity_from_auth_header(&service, "Basic YWxpY2U6YWxpY2UtcGFzcw==")
		.expect_err("basic auth is not implemented, so it must never authenticate");
	assert_eq!(basic, AuthError::InvalidHeader, "a scheme the server cannot speak is a malformed header");

	let digest = extract_identity_from_auth_header(&service, "Digest whatever")
		.expect_err("an unknown scheme must never authenticate");
	assert_eq!(
		basic, digest,
		"basic must be indistinguishable from any other unsupported scheme, or the answer advertises it"
	);
}
