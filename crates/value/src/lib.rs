// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Foundational primitive types for the whole workspace. This crate sits below `core` rather than
//! inside it because `core` itself needs values and diagnostics, which would otherwise cycle. The
//! types here are wire- and disk-stable: rearranging `Value` or `ValueType` corrupts persisted data.

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

mod assertions;

pub mod byte_size;
pub mod clock;
pub mod count;
pub mod encoding;
pub mod error;
pub mod factory;
pub mod fragment;
pub mod params;
pub mod util;
pub mod value;

pub type Result<T> = std::result::Result<T, error::Error>;
