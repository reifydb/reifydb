// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Shared flow execution core: drives a flow's operator graph over a batch of change deltas. Both
//! the transactional (inline pre-commit) and deferred (CDC) paths run through this same code.

mod batch;
mod dispatch;
pub mod reclaim;
mod routing;
mod tick;
mod timers;
