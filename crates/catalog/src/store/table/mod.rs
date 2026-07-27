// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod get_pk_id;
pub mod list;
pub mod set_pk;
pub(crate) mod shape;

use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::common::TimeSource;

use crate::store::{table::shape::table, time_source::read_time_source};

pub(crate) fn decode_table_time(row: &EncodedRow) -> TimeSource {
	read_time_source(&table::SHAPE, row, table::TS)
}
