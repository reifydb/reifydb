// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Built-in functions and procedures the engine VM dispatches to. Functions are pure value transformations evaluated
//! mid-query (math, string, json, temporal, casting); procedures are imperative routines that may mutate catalog or
//! storage state and are invoked as named statements. Both are exposed to RQL through stable qualified names that
//! become wire-visible identifiers.
//!
//! The crate registers every built-in routine with the catalog at boot and resolves user-invoked names against that
//! registry. Renaming a registered routine is a breaking change for queries already on disk, in scripts, and in client
//! code; new routines should be added rather than substituted.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod function;
pub mod procedure;
pub mod routine;
