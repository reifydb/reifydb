// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;
pub mod update;

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::common::TimeSource;

use crate::store::{series::shape::series, time_source::read_time_source};

pub(crate) fn decode_series_time(bytes: &EncodedBytes) -> TimeSource {
	read_time_source(&series::SHAPE, bytes, series::TIME_DOMAIN, series::TS)
}
