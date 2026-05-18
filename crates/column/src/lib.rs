// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Columnar storage engine: the immutable, on-disk representation of materialized columns plus the read-time
//! machinery (compute kernels, predicates, scans, selection vectors, snapshots) the engine uses to query them. This
//! crate owns the bucket layout, the per-column compression and encoding schemes, and the registry that tracks which
//! columns are present and at what version.
//!
//! Read paths come in here, get a column reader, and stream values through compute kernels that operate directly on
//! the encoded bytes where possible - decoding only when a kernel cannot run on the encoded form. The snapshot type
//! is what the subscription tier hands out to consumers so they can iterate over a stable view of the column without
//! racing against ongoing writes.
//!
//! Invariant: a column's encoded bytes plus its stats and bitmap are produced together and never updated piecewise.
//! Tearing those apart - rewriting just the values, just the bitmap, or just the stats - means readers can observe
//! a column whose statistics no longer describe its contents, which silently corrupts every kernel that reads stats
//! to skip work.

pub mod bucket;
pub mod compress;
pub mod compute;
pub mod encoding;
pub mod error;
pub mod predicate;
pub mod reader;
pub mod registry;
pub mod scan;
pub mod selection;
pub mod snapshot;
