// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod batches;
pub mod series;
pub mod table;

use reifydb_type::value::datetime::DateTime;

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
