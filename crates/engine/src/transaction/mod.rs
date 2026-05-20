// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Engine-side helpers around the transaction layer. The `operation/` submodule wraps the typed catalog and
//! storage operations the VM performs inside a transaction so the dispatch handlers can call them by name rather
//! than wiring up the encoded-key plumbing themselves.

#[allow(dead_code)]
pub mod operation;
