// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod extern_c;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub mod extern_load;
#[cfg(feature = "wasm")]
pub mod extern_wasm;
