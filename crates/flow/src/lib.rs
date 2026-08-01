// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Flow execution substrate in two tiers: the lean default carries only what a guest cdylib can name without
//! linking the host, and `runtime` adds `FlowTransaction` plus the `FlowOperator` contract. Lean is the default so a
//! forgotten feature fails the host build loudly rather than linking the catalog, transactions and store into a guest.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod timer;
pub mod window;

#[cfg(feature = "runtime")]
pub mod host;
#[cfg(feature = "runtime")]
pub mod operator;
#[cfg(feature = "runtime")]
pub mod transaction;

#[cfg(all(test, feature = "runtime"))]
pub(crate) mod test_util {
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_transaction::transaction::admin::AdminTransaction;
	use reifydb_value::value::identity::IdentityId;

	pub fn create_test_transaction() -> AdminTransaction {
		let t = TestEngine::new();
		t.begin_admin(IdentityId::system()).unwrap()
	}
}
