// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Version-epoch backstop sampler. The PRIMARY per-commit feed is substrate plumbing and lives in
//! `reifydb_store_multi::gc::epoch::listener`; [`actor`] samples `(now, current version)` on an interval so a commit
//! event dropped under mailbox pressure is still eventually reflected. Same-instant samples collapse to the highest
//! version, so the backstop never lowers a floor the listener already set.

pub mod actor;
