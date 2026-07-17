// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Always-on runtime-metrics subsystem. Samples process memory (RSS anon vs file,
//! private-dirty, PSS), the glibc allocator (live heap vs retained), and ReifyDB
//! internals (buffer key counts, MVCC watermark lag, oracle window count, CDC
//! watermark) and exposes them through a live `system::metrics::runtime::*::current`
//! virtual table per domain, recomputed on query. Callers pull samples on demand via
//! `RuntimeSubsystem::sample_reader`.

#![allow(clippy::tabs_in_doc_comments)]
#![allow(dead_code)]

pub mod collect;
pub mod domain;
pub mod factory;
pub mod subsystem;
pub mod vtable;
