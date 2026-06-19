// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod applier;
#[cfg(not(reifydb_single_threaded))]
pub mod client;
pub mod watermark;
