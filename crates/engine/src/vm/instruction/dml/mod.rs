// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod coerce;
pub(crate) mod context;
pub mod dictionary_insert;
pub mod dispatch;
pub(crate) mod primary_key;
pub(crate) mod returning;
pub mod ringbuffer_delete;
pub mod ringbuffer_insert;
pub mod ringbuffer_update;
pub mod series_delete;
pub mod series_insert;
pub mod series_update;
pub(crate) mod shape;
pub mod table_delete;
pub mod table_insert;
pub mod table_update;
