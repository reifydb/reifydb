// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Test-only helpers shared across the workspace: golden-file harness, the `testscript` runner used by the integration
//! suites, temp-directory and free-port allocators, and small assertion utilities. Production code does not depend
//! on this crate; only `[dev-dependencies]` and the dedicated `testsuite/` repository do.
//!
//! Test scripts here run RQL through the embedded engine; their inputs and outputs are checked against goldenfiles
//! that live in `testsuite/`. Regenerating those goldenfiles is a deliberate manual operation, never an automatic
//! one in this crate.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod goldenfile;
pub mod network;
pub mod tempdir;
pub mod testscript;
pub mod util;
