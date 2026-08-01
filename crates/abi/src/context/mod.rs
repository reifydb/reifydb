// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Per-call context the host fills in for an extension to read, plus the sequential cursors an extension walks
//! input rows with. Valid only for the duration of the call it was handed to.

#[allow(clippy::module_inception)]
pub mod context;
pub mod iterators;
