// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! General storage subsystem crate for ReifyDB. The subsystem itself is always present; each storage strategy lives
//! behind its own feature. The columnar materialization strategy sits in `column/` behind the `column` feature.

pub mod factory;
pub mod subsystem;

#[cfg(feature = "column")]
pub mod column;
