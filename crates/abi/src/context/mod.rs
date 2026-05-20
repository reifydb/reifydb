// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Per-call context carried across the FFI boundary. The host fills in fields the extension may read (clock
//! reference, transaction handle, identity, request-scoped scratch); the iterators submodule defines the
//! sequential cursors the host hands an extension to walk over input rows.

#[allow(clippy::module_inception)]
pub mod context;
pub mod iterators;
