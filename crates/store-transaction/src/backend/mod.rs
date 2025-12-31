// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub(crate) mod delta_optimizer;
pub mod memory;
pub mod primitive;
pub mod result;
pub mod sqlite;
pub mod storage;

pub use primitive::{PrimitiveBackend, PrimitiveStorage, RangeBatch, RawEntry, TableId};
pub use storage::BackendStorage;
