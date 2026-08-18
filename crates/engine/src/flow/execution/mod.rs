// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Shared flow execution core: drives a flow's operator graph over a batch of change deltas. Both the transactional
//! (inline pre-commit) and deferred (CDC) paths run flows through this same code - routing seeds entry nodes from the
//! source registry, dispatch invokes each operator in topological order, and tick drives time-based operator work.

mod batch;
mod dispatch;
mod routing;
mod tick;
