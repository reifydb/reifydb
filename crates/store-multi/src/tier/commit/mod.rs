// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Commit buffer tier of the multi-version store. Holds recent writes in memory before the flusher migrates them to
//! the persistent tier. Reads consult the commit buffer first and fall through to persistent storage on a miss, so
//! freshly-written rows are visible immediately without waiting for the flush.

pub mod buffer;
pub mod memory;
pub mod result;
