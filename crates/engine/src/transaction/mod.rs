// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Engine-side helpers around the transaction layer. The `operation/` submodule wraps the typed catalog and
//! storage operations the VM performs inside a transaction so the dispatch handlers can call them by name rather
//! than wiring up the encoded-key plumbing themselves.

#[allow(dead_code)]
pub mod operation;
