// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;
pub mod update;

use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::common::TimeSource;

use crate::store::{series::shape::series, time_source::read_time_source};

pub(crate) fn decode_series_time(row: &EncodedRow) -> TimeSource {
	read_time_source(&series::SHAPE, row, series::TS)
}
