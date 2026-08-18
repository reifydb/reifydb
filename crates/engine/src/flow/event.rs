// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::change::Change;
use reifydb_value::value::datetime::DateTime;



pub enum FlowEvent {

	Data(Change),

	Tick { timestamp: DateTime },
}
