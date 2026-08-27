// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

mod assertions;

pub mod byte_size;
pub mod clock;
pub mod config;
pub mod count;
pub mod encoding;
pub mod error;
pub mod factory;
pub mod fragment;
pub mod params;
pub mod util;
pub mod value;

pub type Result<T> = std::result::Result<T, error::Error>;
