// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

// #![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod error;
pub mod fragment;
pub mod params;
pub mod storage;
pub mod util;
pub mod value;

/// Result type alias for this crate
pub type Result<T> = std::result::Result<T, error::Error>;
