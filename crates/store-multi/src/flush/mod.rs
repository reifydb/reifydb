// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Background flusher that migrates writes from the buffer tier to the persistent tier. The actor decides when to
//! flush (size, age, explicit request) and the listener exposes flush events to anything that wants to track
//! progress, like the admin UI or test harnesses waiting for durability.

pub mod actor;
pub mod listener;
