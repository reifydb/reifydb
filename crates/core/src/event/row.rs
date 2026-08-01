// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use crate::interface::catalog::{flow::OperatorId, object::ObjectId};

define_event! {

	pub struct RowsExpiredEvent {
		pub objects_scanned: u64,
		pub objects_skipped: u64,
		pub rows_expired: u64,
		pub versions_dropped: u64,
		pub bytes_discovered: HashMap<ObjectId, u64>,
		pub bytes_reclaimed: HashMap<ObjectId, u64>,
	}
}

define_event! {

	pub struct OperatorRowsExpiredEvent {
		pub operators_scanned: u64,
		pub operators_skipped: u64,
		pub rows_expired: u64,
		pub versions_dropped: u64,
		pub bytes_discovered: HashMap<OperatorId, u64>,
		pub bytes_reclaimed: HashMap<OperatorId, u64>,
	}
}
