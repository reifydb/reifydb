// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! GC (garbage collection) events.

use std::collections::HashMap;

use crate::interface::catalog::shape::ShapeId;

define_event! {
	/// Emitted after a TTL GC scan cycle completes.
	pub struct RowsExpiredEvent {
		pub shapes_scanned: u64,
		pub shapes_skipped: u64,
		pub rows_expired: u64,
		pub versions_dropped: u64,
		pub bytes_discovered: HashMap<ShapeId, u64>,
		pub bytes_reclaimed: HashMap<ShapeId, u64>,
	}
}
