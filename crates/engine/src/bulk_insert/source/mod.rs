// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Target-specific builders for tables and ring buffers.

mod ringbuffer;
mod table;

pub use ringbuffer::{PendingRingBufferInsert, RingBufferInsertBuilder};
pub use table::{PendingTableInsert, TableInsertBuilder};
