// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod factory;
pub mod operator;
pub mod timer;
pub mod window;

#[cfg(feature = "runtime")]
pub mod context;
#[cfg(feature = "runtime")]
pub mod engine;
#[cfg(feature = "runtime")]
pub mod error;
#[cfg(feature = "runtime")]
pub mod transaction;
