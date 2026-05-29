// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::datetime::DateTime;

#[derive(Debug, Clone)]
pub enum RowTtlMessage {
	Tick(DateTime),

	Shutdown,
}
