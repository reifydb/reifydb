// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod context;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod ffi;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod native;
