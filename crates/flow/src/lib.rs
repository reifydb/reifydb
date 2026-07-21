// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Flow execution substrate. Holds the `FlowTransaction` accumulator and its operator-state layer
//! (the transaction module) plus the `Operator` contract every flow operator implements. The
//! `reifydb-sub-flow` subsystem builds its operator library and runtime on top of this crate;
//! other subsystems that only need to drive a flow transaction (subscription hydration, test
//! harnesses) depend on this crate directly instead of on the subsystem.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod host;
pub mod operator;
pub mod transaction;

#[cfg(test)]
pub(crate) mod test_util {
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_transaction::transaction::admin::AdminTransaction;
	use reifydb_value::value::identity::IdentityId;

	pub fn create_test_transaction() -> AdminTransaction {
		let t = TestEngine::new();
		t.begin_admin(IdentityId::system()).unwrap()
	}
}
