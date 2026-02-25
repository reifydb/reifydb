// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use serde::{Deserialize, Serialize};

use crate::interface::catalog::id::{MigrationEventId, MigrationId};

/// A migration definition stored in the catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MigrationDef {
	pub id: MigrationId,
	pub name: String,
	/// RQL source text for the migration body
	pub body: String,
	/// Optional RQL source text for the rollback body
	pub rollback_body: Option<String>,
}

/// An audit trail entry for a migration apply or rollback.
/// The CommitVersion is NOT a field — it's the MVCC version key.
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
