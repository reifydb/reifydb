// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod batches;
pub mod series;
pub mod table;

use reifydb_type::value::datetime::DateTime;

// Two distinct message types, one per actor. Same shape today (`Tick`, `Shutdown`)
// but they will diverge in v2 (series grows `MaterializeBucket(BucketId)` from
// change feeds; tables grow `MaterializeCommit(TableId, CommitVersion)`). Keeping
// them separate prevents a table-path message from ever reaching the series
// actor, enforced at compile time.

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
