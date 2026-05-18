// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Generic byte-layout helpers shared by the storage tier, CDC, replication, and the diagnostic formatter.
//!
//! `binary` provides plain serialise and deserialise routines, `format` exposes a pluggable `Formatter` trait used by
//! tools and tests to render keys and values in a human-readable form, and `keycode` is the order-preserving codec that
//! turns typed keys into the bytes that go on disk.

pub mod binary;
pub mod format;
pub mod keycode;
