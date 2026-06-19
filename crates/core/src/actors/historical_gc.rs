// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::datetime::DateTime;

#[derive(Debug, Clone)]
pub enum HistoricalGcMessage {
	Tick(DateTime),
	ContinueSweep,
	Shutdown,
}
