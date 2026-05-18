// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Foundational primitive types shared by every other crate in the workspace - the value enum, type enum, type
//! constraints, the source-fragment carrier used in diagnostics, the error and diagnostic machinery, and the parameter
//! binding shape used at the wire boundary. This crate is the bottom of the dependency graph; nothing here depends on
//! `core` or any other ReifyDB crate.
//!
//! Anything that wants a stable representation of a column value, a typed identifier, a parameter list, or a
//! diagnostic anchor uses this crate. The reason it sits below `core` rather than inside it is to break what would
//! otherwise be a cycle: `core` itself needs values and diagnostics.
//!
//! Invariant: types declared here are wire-stable and on-disk-stable. Adding a variant to `Type` or `Value` is a
//! workspace-wide change that requires bumping wire-format and storage encodings; rearranging the existing variants
//! silently corrupts persisted data and cross-version replication.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod error;
pub mod fragment;
pub mod params;
pub mod storage;
pub mod util;
pub mod value;

pub type Result<T> = std::result::Result<T, error::Error>;
