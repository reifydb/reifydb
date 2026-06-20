// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Persistent free-page reclamation. The GC and flush actors delete rows and checkpoint the WAL, but with
//! `auto_vacuum=INCREMENTAL` the freed pages are only returned to the OS by an explicit `incremental_vacuum`. This
//! actor runs that reclaim on its own interval, decoupled from the delete/flush cadence so a heavyweight vacuum +
//! WAL truncate never piggybacks on every eviction.

pub mod actor;
