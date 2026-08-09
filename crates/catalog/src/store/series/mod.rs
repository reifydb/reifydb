// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
pub mod create;
pub mod drop;
pub mod find;
pub mod get;
pub mod list;
pub(crate) mod shape;
pub mod update;

use reifydb_core::common::TimeSource;

use crate::store::{series::shape::series, time_source::read_time_source};

pub(crate) fn decode_series_time(bytes: &EncodedCatalogRow) -> TimeSource {
	read_time_source(&series::SHAPE, bytes.as_slice(), series::TIME_DOMAIN, series::TS)
}
