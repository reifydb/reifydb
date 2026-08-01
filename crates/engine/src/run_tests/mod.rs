// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Engine-side runner for CREATE TEST / RUN TESTS. Tests execute inside the same transaction as the admin
//! command that launched them, so their side effects live and die with that outer transaction.

pub mod result;
pub(crate) mod run;
