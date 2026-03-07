// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::engine::StandardEngine;
use reifydb_type::value::identity::IdentityId;

use super::retry::RetryPolicy;

pub struct AdminSession {
	pub(crate) engine: StandardEngine,
	pub(crate) identity: IdentityId,
	pub(crate) retry: RetryPolicy,
}
