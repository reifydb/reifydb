// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! DML instruction handlers. INSERT, UPDATE, DELETE specialised per shape - tables, ringbuffers, series, the
//! dictionary, and the RETURNING-style read-back path. Per-shape handling is necessary because each shape stores
//! its rows under a different encoded-key layout and has different uniqueness and ordering invariants the
//! dispatcher must respect before committing.

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
