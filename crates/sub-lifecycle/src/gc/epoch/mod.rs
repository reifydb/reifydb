// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Version-epoch machinery: the time-to-version map every TTL resolves through.
//!
//! [`durable`] owns the map's lifecycle - it persists one sample per wall-clock bucket, prunes beyond the retention
//! horizon, and hydrates RAM at boot, so coverage depends on the declared horizon rather than on process uptime.
//! The in-RAM map itself is filled by the commit path, which records every assigned version into the open bucket,
//! so resolution follows the bucket width rather than any sampling cadence.

pub mod durable;
pub mod log;
