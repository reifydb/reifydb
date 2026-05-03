// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Pure column transforms: append, filter, and take.
//!
//! Each transform produces a new column from one or more inputs without mutating the inputs. These are the building
//! blocks the engine composes when it cannot rewrite an operation as a buffer-level update.

pub mod append;
pub mod filter;
pub mod take;
