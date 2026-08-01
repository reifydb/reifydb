// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Version-space reclamation executors: `historical` reclaims versions below the read watermark and `epoch` keeps
//! the time-to-version map. Row and operator expiry compare a row's own timestamp against a cutoff instant
//! instead, and operator state is reclaimed by the flow engine's group pass rather than here.

pub mod epoch;
pub mod historical;
