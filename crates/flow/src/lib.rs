// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Flow execution substrate, in two tiers.
//!
//! The default tier is lean: types a guest operator compiled to a cdylib can name without linking
//! the host. The `runtime` feature adds the `FlowTransaction` accumulator and its operator-state
//! layer plus the `Operator` contract every flow operator implements, and is what the
//! `reifydb-sub-flow` subsystem builds its operator library on. Other subsystems that only need to
//! drive a flow transaction (subscription hydration, test harnesses) enable `runtime` too.
//!
//! The lean tier is the default so that forgetting the feature breaks a host build loudly on a
//! missing `flow::transaction`, rather than silently linking the catalog, the transaction layer and
//! the store into a guest cdylib.

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
