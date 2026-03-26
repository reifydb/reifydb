// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::change::Change;
use reifydb_type::value::datetime::DateTime;

/// An event flowing through the flow pipeline.
/// Either a data change or a periodic tick for time-based maintenance.
pub enum FlowEvent {
	/// A data change (insert/update/remove)
	Data(Change),
	/// Periodic tick carrying current system time
	Tick { timestamp: DateTime },
}
