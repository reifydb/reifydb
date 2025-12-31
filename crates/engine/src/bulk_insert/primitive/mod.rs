// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Target-specific builders for tables and ring buffers.

mod ringbuffer;
mod table;

pub use ringbuffer::{PendingRingBufferInsert, RingBufferInsertBuilder};
pub use table::{PendingTableInsert, TableInsertBuilder};
