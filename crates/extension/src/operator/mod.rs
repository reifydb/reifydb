// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod callbacks;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod extern_c;
