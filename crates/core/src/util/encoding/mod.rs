// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Generic byte-layout helpers shared by the storage tier, CDC, replication, and the diagnostic formatter.
//!
//! `binary` provides plain serialise and deserialise routines and `format` exposes a pluggable `Formatter` trait used
//! by tools and tests to render keys and values in a human-readable form. The order-preserving key codec itself lives
//! in `reifydb-codec` and is re-exported from `crate::key` as `keycode`.

pub mod binary;
pub mod format;
