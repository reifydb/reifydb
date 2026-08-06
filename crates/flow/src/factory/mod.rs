// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Constructors for test fixtures built on flow-side types, mirroring `reifydb_value::factory` for the types
//! that crate cannot name. Where the argument is a raw integer the unit lives in the function name, since the
//! type cannot carry it.

pub mod coord;

pub use coord::event_coord_at_millis;
