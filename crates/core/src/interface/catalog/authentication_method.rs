// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_value::value::duration::Duration;
use serde::{Deserialize, Serialize};

pub type AuthenticationMethodId = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthenticationMethod {
	pub id: AuthenticationMethodId,
	pub name: String,
	pub ttl: Option<Duration>,
	pub promote_guest: bool,
	pub properties: BTreeMap<String, String>,
}

pub struct AuthenticationMethodToCreate {
	pub name: String,
	pub ttl: Option<Duration>,
	pub promote_guest: bool,
	pub properties: BTreeMap<String, String>,
}
