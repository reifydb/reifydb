// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

#[cfg(feature = "runtime")]
pub mod aggregate;
#[cfg(feature = "runtime")]
pub mod analyzer;
pub mod backfill;
#[cfg(feature = "runtime")]
pub mod compiler;
#[cfg(feature = "runtime")]
pub mod context;
#[cfg(feature = "runtime")]
pub mod error;
#[cfg(feature = "runtime")]
pub mod operator;
#[cfg(feature = "runtime")]
pub mod time_domain;
