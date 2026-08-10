// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_target = "native")]
pub mod context;
#[cfg(reifydb_target = "native")]
pub mod ffi;
#[cfg(reifydb_target = "native")]
pub mod native;
