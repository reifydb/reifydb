// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod alter;
mod create;
mod drop;

use reifydb_core::interface::catalog::authentication_method::AuthenticationMethod;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::identity::IdentityId;

const GITHUB: &str = "CREATE AUTHENTICATION METHOD github WITH { ttl: 30d, promote_guest: true, client_id: 'abc', redirect_uri: 'https://app/cb' }";

fn github_engine() -> TestEngine {
	TestEngine::builder().with_cdc().with_github_client_secret("test-secret").build()
}

fn committed(t: &TestEngine, name: &str) -> Option<AuthenticationMethod> {
	let mut query = t.begin_query(IdentityId::system()).unwrap();
	t.catalog().find_authentication_method_by_name(&mut Transaction::Query(&mut query), name).unwrap()
}
