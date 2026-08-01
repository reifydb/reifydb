// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Version-epoch machinery: a time-to-version map. Row and operator expiry no longer read it; they compare a row's
//! own timestamp against a cutoff instant.
//!
//! [`durable`] persists one sample per wall-clock bucket, prunes beyond the retention horizon and hydrates RAM at
//! boot, so coverage follows the declared horizon rather than process uptime.

pub mod durable;
pub mod log;
