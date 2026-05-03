// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::change::Change;
use reifydb_type::value::datetime::DateTime;



pub enum FlowEvent {

	Data(Change),

	Tick { timestamp: DateTime },
}
