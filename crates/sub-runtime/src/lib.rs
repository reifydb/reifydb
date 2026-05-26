// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Always-on runtime-metrics subsystem. Samples process memory (RSS anon vs file,
//! private-dirty, PSS), the glibc allocator (live heap vs retained), and ReifyDB
//! internals (buffer key counts, MVCC watermark lag, oracle window count, CDC
//! watermark) and exposes them two ways - mirroring the profiler: a live
//! `system::metrics::runtime::memory::current` virtual table recomputed on query,
//! and a `system::metrics::runtime::memory::snapshots` series written each tick by
//! a background sampler. The history sampler is gated on the optional
//! `RuntimeMetricsInterval` config: when unset, only the live view is available.

#![allow(clippy::tabs_in_doc_comments)]
#![allow(dead_code)]

pub mod actor;
pub mod collect;
pub mod domain;
pub mod factory;
pub mod subsystem;
pub mod vtable;
