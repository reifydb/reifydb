// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Test-only helpers shared across the workspace: golden-file harness, the `testscript` runner,
//! temp-directory and free-port allocators, and small assertion utilities. Every dependent takes
//! it as a `[dev-dependencies]` entry, so no production build resolves it.
//!
//! Goldenfile regeneration is opt-in through `UPDATE_TESTFILES` and its aliases; nothing here
//! ever rewrites a goldenfile on its own.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

extern crate self as reifydb_testing;

pub mod goldenfile;
pub mod network;
pub mod tempdir;
pub mod testscript;
pub mod util;
