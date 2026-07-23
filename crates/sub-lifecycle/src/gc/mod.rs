// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Version-space reclamation executors. `historical` reclaims versions below the read watermark, `operator`
//! applies per-flow retention overrides to operator state, and `epoch` keeps the time-to-version map that makes
//! version-anchored expiry possible.

pub mod epoch;
pub mod historical;
pub mod operator;
