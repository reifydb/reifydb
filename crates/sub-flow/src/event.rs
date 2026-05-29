// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::change::Change;
use reifydb_value::value::datetime::DateTime;



pub enum FlowEvent {

	Data(Change),

	Tick { timestamp: DateTime },
}
