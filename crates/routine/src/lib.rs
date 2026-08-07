// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Built-in functions (pure, evaluated mid-query) and procedures (imperative, may mutate catalog or storage), all
//! registered with the catalog at boot.
//!
//! The qualified name of a routine is wire-visible: renaming one breaks queries already on disk, in scripts and in
//! client code, so add rather than substitute.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod function;
pub mod monoid;
pub mod procedure;
