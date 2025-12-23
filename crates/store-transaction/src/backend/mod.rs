// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) mod delta_optimizer;
pub mod memory;
pub mod primitive;
pub mod result;
pub mod sqlite;
pub mod storage;

pub use primitive::{PrimitiveBackend, PrimitiveStorage, RangeBatch, RawEntry, TableId};
pub use storage::BackendStorage;
