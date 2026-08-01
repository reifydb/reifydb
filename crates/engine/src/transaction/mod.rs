// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Wraps the typed catalog and storage operations the VM performs inside a transaction, so dispatch handlers
//! call them by name instead of each wiring up the encoded-key plumbing itself.

#[allow(dead_code)]
pub mod operation;
