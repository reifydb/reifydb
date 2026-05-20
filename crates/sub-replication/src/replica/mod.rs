// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod applier;
#[cfg(not(reifydb_single_threaded))]
pub mod client;
pub mod watermark;
