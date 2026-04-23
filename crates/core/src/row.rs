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

/// TTL (Time-To-Live) configuration for automatic row expiration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowTtl {
	/// Duration in nanoseconds after which rows expire
	pub duration_nanos: u64,
	/// Which row timestamp to measure from
	pub anchor: RowTtlAnchor,
	/// How expired rows are cleaned up
	pub cleanup_mode: RowTtlCleanupMode,
}

/// Which row timestamp the TTL duration is measured from
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RowTtlAnchor {
	/// Measure from `created_at` (default)
	#[default]
	Created,

	/// Measure from `updated_at` - updates extend the row's lifetime
	Updated,
}

/// How expired rows are cleaned up
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowTtlCleanupMode {
	/// Create tombstones and CDC entries - maintains audit trail
	Delete,

	/// Silent removal from storage - no CDC entries, no tombstones
	Drop,
}
