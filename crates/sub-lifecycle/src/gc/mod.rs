// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Version-space reclamation executors. `historical` reclaims versions below the read watermark, and `epoch`
//! keeps the time-to-version map. Row and operator expiry no longer read it: both compare a row's own
//! timestamp against a cutoff instant. Operator state is no longer
//! reclaimed here: it is reclaimed by the flow engine's two-phase group pass, which reaches it through the
//! group ranges the state now lives in.

pub mod epoch;
pub mod historical;
