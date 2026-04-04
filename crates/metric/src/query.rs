// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::fingerprint::StatementFingerprint;
use serde::{Deserialize, Serialize};

/// Broad classification of a query statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueryKind {
	From,
	Insert,
	Update,
	Delete,
	Create,
	Drop,
	Alter,
}

/// A single query execution record, emitted after each statement completes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRecord {
	pub fingerprint: StatementFingerprint,
	pub normalized_rql: String,
	pub kind: QueryKind,
	pub duration_us: u64,
	pub compute_us: u64,
	pub rows_affected: u64,
	pub success: bool,
	pub timestamp: u64,
}
