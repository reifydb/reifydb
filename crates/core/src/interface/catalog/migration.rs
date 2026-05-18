// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_runtime::hash::{Hash128, xxh3_128};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{MigrationEventId, MigrationId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Migration {
	pub id: MigrationId,
	pub name: String,

	pub body: String,

	pub rollback_body: Option<String>,

	pub hash: Hash128,
}

pub fn migration_hash(body: &str, rollback_body: Option<&str>) -> Hash128 {
	let mut buf = Vec::with_capacity(body.len() + 1 + rollback_body.map(|r| r.len()).unwrap_or(0));
	buf.extend_from_slice(body.as_bytes());
	buf.push(0);
	if let Some(rb) = rollback_body {
		buf.extend_from_slice(rb.as_bytes());
	}
	xxh3_128(&buf)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MigrationEvent {
	pub id: MigrationEventId,
	pub migration_id: MigrationId,
	pub action: MigrationAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationAction {
	Applied,
	Rollback,
}
