// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod batches;
pub mod series;
pub mod table;

use reifydb_value::value::datetime::DateTime;

#[derive(Clone, Debug)]
pub enum TableMessage {
	Tick(DateTime),
	Shutdown,
}

#[derive(Clone, Debug)]
pub enum SeriesMessage {
	Tick(DateTime),
	Shutdown,
}
