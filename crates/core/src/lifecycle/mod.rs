// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Shared data-lifecycle vocabulary. Every contract that both the lifecycle subsystem and the layers it drives must
//! agree on lives here: the slice/progress protocol tasks report against, the registry any crate can hand work to,
//! the floor traits lower layers implement to hold reclamation back, and the metrics reclamation reports.
//!
//! Contracts only. The executors that act on them live in `reifydb-sub-lifecycle`; keeping the vocabulary in core is
//! what lets engine, catalog, and store-multi participate in the lifecycle without depending on a subsystem.

pub mod epoch;
pub mod metrics;
pub mod operator;
pub mod progress;
pub mod registry;
pub mod task;
pub mod watermark;
