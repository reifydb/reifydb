// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Version-epoch machinery: the time-to-version map every TTL resolves through.
//!
//! [`durable`] owns the map's lifecycle - it persists one sample per wall-clock bucket, prunes beyond the retention
//! horizon, and hydrates RAM at boot, so coverage depends on the declared horizon rather than on process uptime.
//! [`actor`] is a periodic in-RAM backstop sampling `(now, current version)` between durable buckets, giving finer
//! resolution than the bucket width without persisting a sample per commit. Same-instant samples collapse to the
//! highest version, so a backstop sample can never lower a floor already established.

pub mod actor;
pub mod durable;
pub mod log;
