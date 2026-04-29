// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_runtime::hash::{Hash128, xxh3_128};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{MigrationEventId, MigrationId};

/// A migration definition stored in the catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Migration {
	pub id: MigrationId,
	pub name: String,
	/// RQL source text for the migration body
	pub body: String,
	/// Optional RQL source text for the rollback body
	pub rollback_body: Option<String>,
	/// Content hash of `body || 0x00 || rollback_body.unwrap_or("")`.
	/// Used to detect post-registration tampering: re-registering a migration
	/// whose name is already known but whose hash differs is rejected.
	pub hash: Hash128,
}

/// Compute the content hash for a migration body and optional rollback body.
///
/// The two strings are joined with a NUL byte (which is not a valid character
/// in RQL source) so that `body="A", rollback="B"` and `body="", rollback="AB"`
/// produce different hashes.
pub fn migration_hash(body: &str, rollback_body: Option<&str>) -> Hash128 {
	let mut buf = Vec::with_capacity(body.len() + 1 + rollback_body.map(|r| r.len()).unwrap_or(0));
	buf.extend_from_slice(body.as_bytes());
	buf.push(0);
	if let Some(rb) = rollback_body {
		buf.extend_from_slice(rb.as_bytes());
	}
	xxh3_128(&buf)
}

/// An audit trail entry for a migration apply or rollback.
/// The CommitVersion is NOT a field - it's the MVCC version key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MigrationEvent {
	pub id: MigrationEventId,
	pub migration_id: MigrationId,
	pub action: MigrationAction,
}

/// The type of migration action recorded in the audit trail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationAction {
	Applied,
	Rollback,
}
