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
pub mod update;

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::common::TimeSource;

use crate::store::{ringbuffer::shape::ringbuffer, time_source::read_time_source};

pub(crate) fn decode_ringbuffer_time(bytes: &EncodedBytes) -> TimeSource {
	read_time_source(&ringbuffer::SHAPE, bytes, ringbuffer::TIME_DOMAIN, ringbuffer::TS)
}
