// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::row_number::RowNumber;
use serde::{Deserialize, Serialize};

use crate::encoded::{row::EncodedRow, shape::RowShape};

#[derive(Debug, Clone)]
pub struct Row {
	pub number: RowNumber,
	pub encoded: EncodedRow,
	pub shape: RowShape,
}

/// TTL (Time-To-Live) configuration for automatic expiration.
///
/// Used both for row-level TTL on data shapes (tables, views, series) and for
/// operator-state retention on streaming operators (Distinct, Join). The shape
/// of the config (`duration` + `anchor` + `cleanup_mode`) is identical;
/// consumers interpret the fields per their own semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ttl {
	/// Duration in nanoseconds after which entries expire
	pub duration_nanos: u64,
	/// Which timestamp the duration is measured from
	pub anchor: TtlAnchor,
	/// How expired entries are cleaned up
	pub cleanup_mode: TtlCleanupMode,
}

/// Which timestamp the TTL duration is measured from
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TtlAnchor {
	/// Measure from `created_at` (default)
	#[default]
	Created,

	/// Measure from `updated_at` - updates extend the entry's lifetime
	Updated,
}

/// How expired entries are cleaned up
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TtlCleanupMode {
	/// Create tombstones and CDC entries - maintains audit trail
	Delete,

	/// Silent removal from storage - no CDC entries, no tombstones
	Drop,
}
